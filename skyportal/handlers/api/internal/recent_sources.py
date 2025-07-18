from collections import defaultdict

import sqlalchemy as sa
from sqlalchemy import desc
from sqlalchemy.orm import joinedload

from baselayer.app.access import auth_or_token
from baselayer.app.env import load_env
from baselayer.log import make_log
from skyportal.models.group import Group

from ....models import Obj, ObjTag, Source
from ....utils.parse import get_list_typed
from ...base import BaseHandler
from .source_views import t_index

# maxNumSources is the maximum number of sources to return
# includeSitewide is a boolean that determines whether to include
# sources that are only in the sitewide group
default_prefs = {
    "maxNumSources": 25,
    "includeSitewideSources": False,
    "groupIds": [],
}

env, cfg = load_env()
log = make_log("api/recent_sources")


class RecentSourcesHandler(BaseHandler):
    @classmethod
    def get_recent_source_ids(cls, current_user, session):
        user_prefs = getattr(current_user, "preferences", None) or {}
        recent_sources_prefs = user_prefs.get("recentSources", {})
        recent_sources_prefs = {**default_prefs, **recent_sources_prefs}

        max_num_sources = int(recent_sources_prefs["maxNumSources"])
        include_sitewide = recent_sources_prefs.get("includeSitewideSources", False)
        group_ids = get_list_typed(
            recent_sources_prefs.get("groupIds", []),
            int,
            error_msg="Invalid group_ids, must be a list of integers.",
        )

        stmt = Source.select(session.user_or_token).where(Source.active.is_(True))

        if len(group_ids) > 0:
            stmt = stmt.where(Source.group_id.in_(group_ids))
        elif not include_sitewide:
            public_group_id = session.scalar(
                sa.select(Group.id).where(Group.name == cfg["misc.public_group_name"])
            )
            if public_group_id is None:
                raise ValueError(
                    f"Could not find public group with name {cfg['misc.public_group_name']}"
                )
            stmt = stmt.where(Source.group_id != public_group_id)

        stmt = stmt.order_by(desc(Source.created_at)).distinct(
            Source.obj_id, Source.created_at
        )
        query_results = session.scalars(stmt.limit(max_num_sources)).all()
        ids = [src.obj_id for src in query_results]
        return ids

    @auth_or_token
    def get(self):
        with self.Session() as session:
            query_results = RecentSourcesHandler.get_recent_source_ids(
                self.current_user, session
            )
            tags = session.scalars(
                ObjTag.select(session.user_or_token).where(
                    ObjTag.obj_id.in_(list(set(query_results)))
                )
            ).all()
            tags = [{**tag.to_dict(), "name": tag.objtagoption.name} for tag in tags]
            # make it a hashmap of obj_id to tags
            tags_dict = defaultdict(list)
            for tag in tags:
                tags_dict[tag["obj_id"]].append(tag)

            sources = []
            sources_seen = defaultdict(lambda: 1)
            for obj_id in query_results:
                # The recency_index is how current a source row was saved for a given
                # object. If recency_index = 0, this is the most recent time a source
                # was saved; recency_index = 1 is the second-latest time the source
                # was saved, etc.
                recency_index = 0
                if obj_id in sources_seen:
                    recency_index = sources_seen[obj_id]
                    sources_seen[obj_id] += 1

                s = session.scalars(
                    Obj.select(
                        session.user_or_token, options=[joinedload(Obj.thumbnails)]
                    ).where(Obj.id == obj_id)
                ).first()

                # Get the entry in the Source table to get the accurate saved_at time
                source_entry = session.scalars(
                    Source.select(session.user_or_token)
                    .where(Source.obj_id == obj_id)
                    .order_by(desc(Source.created_at))
                    .offset(recency_index)
                ).first()

                if s is None or source_entry is None:
                    log(f"Source with obj_id {obj_id} not found.")
                    continue

                sources.append(
                    {
                        "obj_id": s.id,
                        "ra": s.ra,
                        "dec": s.dec,
                        "created_at": source_entry.created_at,
                        "thumbnails": [
                            {
                                "type": t.type,
                                "is_grayscale": t.is_grayscale,
                                "public_url": t.public_url,
                            }
                            for t in sorted(s.thumbnails, key=lambda t: t_index(t.type))
                        ],
                        "classifications": s.classifications,
                        "recency_index": recency_index,
                        "tns_name": s.tns_name,
                        "tags": tags_dict.get(s.id, []),
                    }
                )

            for source in sources:
                num_times_seen = sources_seen[source["obj_id"]]
                # If this source was saved multiple times recently, and this is not
                # the oldest instance of an object being saved (highest recency_index)
                if num_times_seen > 1 and source["recency_index"] != num_times_seen - 1:
                    source["resaved"] = True
                else:
                    source["resaved"] = False
                # Delete bookkeeping recency_index key
                del source["recency_index"]

            return self.success(data=sources)
