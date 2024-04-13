import asyncio
import json
import operator  # noqa: F401
import time
from threading import Thread

import arrow
import gcn
import requests
import sqlalchemy as sa
import tornado.escape
import tornado.ioloop
import tornado.web
from twilio.rest import Client as TwilioClient
from twilio.twiml.voice_response import Say, VoiceResponse

from baselayer.app.env import load_env
from baselayer.app.flow import Flow
from baselayer.app.models import init_db
from baselayer.log import make_log
from skyportal.app_utils import get_app_base_url
from skyportal.email_utils import send_email
from skyportal.models import (
    Allocation,
    AnalysisService,
    Classification,
    Comment,
    DBSession,
    EventObservationPlan,
    FollowupRequest,
    GcnEvent,
    GcnNotice,
    GcnTag,
    Group,
    GroupAdmissionRequest,
    GroupUser,
    Listing,
    Localization,
    ObjAnalysis,
    ObservationPlanRequest,
    Source,
    Shift,
    ShiftUser,
    Spectrum,
    User,
    UserNotification,
)
from skyportal.utils.gcn import get_skymap_properties
from skyportal.utils.notifications import (
    gcn_email_notification,
    gcn_notification_content,
    gcn_slack_notification,
    source_email_notification,
    source_notification_content,
    source_slack_notification,
)

env, cfg = load_env()
log = make_log('notification_queue')

init_db(**cfg['database'])


account_sid = cfg["twilio.sms_account_sid"]
auth_token = cfg["twilio.sms_auth_token"]
from_number = cfg["twilio.from_number"]
client = None
if account_sid and auth_token and from_number:
    client = TwilioClient(account_sid, auth_token)

email = False
if cfg.get("email_service") == "sendgrid" or cfg.get("email_service") == "smtp":
    email = True


op_options = [
    "lt",
    "le",
    "eq",
    "ne",
    "ge",
    "gt",
]


def notification_resource_type(target):
    if not target["notification_type"]:
        return None
    if (
        "favorite_sources" not in target["notification_type"]
        and "gcn_events" not in target["notification_type"]
        and "sources" not in target["notification_type"]
    ):
        return target["notification_type"]
    elif "favorite_sources" in target["notification_type"]:
        return "favorite_sources"
    elif "gcn_events" in target["notification_type"]:
        return "gcn_events"
    elif "sources" in target["notification_type"]:
        return "sources"


def user_preferences(target, notification_setting, resource_type):
    if not isinstance(notification_setting, str):
        return
    if not isinstance(resource_type, str):
        return
    if not target["user"]:
        return

    if notification_setting == "email":
        if not email:
            return
        if not target["user"]["contact_email"]:
            return
        # this ensures that an email is sent regardless of the user's preferences
        # this is useful for group_admission_requests, where we want the admins to always be notified by email
        if resource_type in ['group_admission_request']:
            return True

    if "preferences" not in target["user"]:
        return

    if notification_setting in ["sms", "phone"]:
        if client is None:
            return
        if not target["user"]["contact_phone"]:
            return

    if notification_setting == "slack":
        if not target["user"]["preferences"].get('slack_integration'):
            return
        if not target["user"]["preferences"]['slack_integration'].get("active"):
            return
        if (
            not target["user"]["preferences"]['slack_integration']
            .get("url", "")
            .startswith(cfg["slack.expected_url_preamble"])
        ):
            return

    prefs = target["user"]["preferences"].get('notifications')
    if not prefs:
        return
    else:
        if resource_type in [
            'sources',
            'favorite_sources',
            'gcn_events',
            'followup_requests',
            'mention',
            'analysis_services',
            'observation_plans',
        ]:
            if not prefs.get(resource_type, False):
                return
            if not prefs[resource_type].get(notification_setting, False):
                return
            if not prefs[resource_type][notification_setting].get("active", False):
                return

        return prefs


def user_preferences(target, notification_setting, resource_type):
    if not isinstance(notification_setting, str):
        return
    if not isinstance(resource_type, str):
        return
    if not target["user"]:
        return

    if notification_setting == "email":
        if not email:
            return
        if not target["user"]["contact_email"]:
            return
        # this ensures that an email is sent regardless of the user's preferences
        # this is useful for group_admission_requests, where we want the admins to always be notified by email
        if resource_type in ['group_admission_request']:
            return True

    if "preferences" not in target["user"]:
        return

    if notification_setting in ["sms", "phone"]:
        if client is None:
            return
        if not target["user"]["contact_phone"]:
            return

    if notification_setting == "slack":
        if not target["user"]["preferences"].get('slack_integration'):
            return
        if not target["user"]["preferences"]['slack_integration'].get("active"):
            return
        if (
            not target["user"]["preferences"]['slack_integration']
            .get("url", "")
            .startswith(cfg["slack.expected_url_preamble"])
        ):
            return

    prefs = target["user"]["preferences"].get('notifications')
    if not prefs:
        return
    else:
        if resource_type in [
            'sources',
            'favorite_sources',
            'gcn_events',
            'followup_requests',
            'mention',
            'analysis_services',
            'observation_plans',
        ]:
            if not prefs.get(resource_type, False):
                return
            if not prefs[resource_type].get(notification_setting, False):
                return
            if not prefs[resource_type][notification_setting].get("active", False):
                return

        return prefs


def send_slack_notification(target):
    resource_type = notification_resource_type(target)
    notifications_prefs = user_preferences(target, "slack", resource_type)
    if not notifications_prefs:
        return
    integration_url = target["user"]["preferences"]['slack_integration'].get('url')

    slack_microservice_url = f'http://127.0.0.1:{cfg["slack.microservice_port"]}'

    app_url = get_app_base_url()

    try:
        if resource_type == 'gcn_events':
            data = json.dumps(
                {
                    "url": integration_url,
                    "blocks": gcn_slack_notification(
                        target=target,
                        data=target["content"],
                        new_tag=(target["notification_type"] == "gcn_events_new_tag"),
                    ),
                }
            )
        elif resource_type == 'sources':
            data = json.dumps(
                {
                    "url": integration_url,
                    "blocks": source_slack_notification(
                        target=target, data=target["content"]
                    ),
                }
            )
        else:
            data = json.dumps(
                {
                    "url": integration_url,
                    "text": f'{target["text"]} ({app_url}{target["url"]})',
                }
            )

        requests.post(
            slack_microservice_url,
            data=data,
            headers={'Content-Type': 'application/json'},
        )
        log(
            f'Sent slack notification to user {target["user"]["id"]} at slack_url: {integration_url}, body: {target["text"]}, resource_type: {resource_type}'
        )
    except Exception as e:
        log(f"Error sending slack notification: {e}")


def send_email_notification(target):
    resource_type = notification_resource_type(target)
    prefs = user_preferences(target, "email", resource_type)

    if not prefs:
        return

    subject = None
    body = None

    app_url = get_app_base_url()

    try:
        if resource_type == "sources":
            subject, body = source_email_notification(
                target=target, data=target["content"]
            )
        elif resource_type == "gcn_events":
            subject, body = gcn_email_notification(
                target=target,
                data=target["content"],
                new_tag=(target["notification_type"] == "gcn_events_new_tag"),
            )

        elif resource_type == "followup_requests":
            subject = f"{cfg['app.title']} - New follow-up request"

        elif resource_type == "observation_plans":
            subject = f"{cfg['app.title']} - New observation plan"

        elif resource_type == "analysis_services":
            subject = f"{cfg['app.title']} - New completed analysis service"

        elif resource_type == "favorite_sources":
            if target["notification_type"] == "favorite_sources_new_classification":
                subject = (
                    f"{cfg['app.title']} - New classification on a favorite source"
                )
            elif target["notification_type"] == "favorite_sources_new_spectrum":
                subject = f"{cfg['app.title']} - New spectrum on a favorite source"
            elif target["notification_type"] == "favorite_sources_new_comment":
                subject = f"{cfg['app.title']} - New comment on a favorite source"
            elif target["notification_type"] == "favorite_sources_new_activity":
                subject = f"{cfg['app.title']} - New activity on a favorite source"

        elif resource_type == "mention":
            subject = f"{cfg['app.title']} - User mentioned you in a comment"

        elif resource_type == "group_admission_request":
            subject = f"{cfg['app.title']} - New group admission request"

        if subject and target["user"]["contact_email"]:
            try:
                if body is None:
                    body = f'{target["text"]} ({app_url}{target["url"]})'
                send_email(
                    recipients=[target["user"]["contact_email"]],
                    subject=subject,
                    body=body,
                )
                log(
                    f'Sent email notification to user {target["user"]["id"]} at email: {target["user"]["contact_email"]}, subject: {subject}, body: {body}, resource_type: {resource_type}'
                )
            except Exception as e:
                log(f"Error sending email notification: {e}")

    except Exception as e:
        log(f"Error sending email notification: {e}")


def send_sms_notification(target):
    resource_type = notification_resource_type(target)
    prefs = user_preferences(target, "sms", resource_type)
    if not prefs:
        return

    sending = False
    if prefs[resource_type]['sms'].get("on_shift", False):
        current_shift = (
            Shift.query.join(ShiftUser)
            .filter(ShiftUser.user_id == target["user"]["id"])
            .filter(Shift.start_date <= arrow.utcnow().datetime)
            .filter(Shift.end_date >= arrow.utcnow().datetime)
            .first()
        )
        if current_shift is not None:
            sending = True

    timeslot = prefs[resource_type]['sms'].get("time_slot", [])
    if len(timeslot) > 0:
        current_time = arrow.utcnow().datetime
        if timeslot[0] < timeslot[1]:
            if current_time.hour >= timeslot[0] and current_time.hour <= timeslot[1]:
                sending = True
        else:
            if current_time.hour <= timeslot[1] or current_time.hour >= timeslot[0]:
                sending = True

    if sending:
        try:
            client.messages.create(
                body=f'{cfg["app.title"]} - {target["text"]}',
                from_=from_number,
                to=target["user"]["contact_phone"].e164,
            )
            log(
                f'Sent SMS notification to user {target["user"]["id"]} at phone number: {target["user"]["contact_phone"].e164}, body: {target["text"]}, resource_type: {resource_type}'
            )
        except Exception as e:
            log(f"Error sending sms notification: {e}")


def send_phone_notification(target):
    resource_type = notification_resource_type(target)
    prefs = user_preferences(target, "phone", resource_type)

    if not prefs:
        return

    sending = False
    if prefs[resource_type]['phone'].get("on_shift", False):
        current_shift = (
            Shift.query.join(ShiftUser)
            .filter(ShiftUser.user_id == target["user"]["id"])
            .filter(Shift.start_date <= arrow.utcnow().datetime)
            .filter(Shift.end_date >= arrow.utcnow().datetime)
            .first()
        )
        if current_shift is not None:
            sending = True

    timeslot = prefs[resource_type]['phone'].get("time_slot", [])
    if len(timeslot) > 0:
        current_time = arrow.utcnow().datetime
        if timeslot[0] < timeslot[1]:
            if current_time.hour >= timeslot[0] and current_time.hour <= timeslot[1]:
                sending = True
        else:
            if current_time.hour <= timeslot[1] or current_time.hour >= timeslot[0]:
                sending = True

    if sending:
        try:
            message = f'Greetings. This is the SkyPortal robot. {target["text"]}'
            client.calls.create(
                twiml=VoiceResponse().append(Say(message=message)),
                from_=from_number,
                to=target["user"]["contact_phone"].e164,
            )
            log(
                f'Sent Phone Call notification to user {target["user"]["id"]} at phone number: {target["user"]["contact_phone"].e164}, message: {message}, resource_type: {resource_type}'
            )
        except Exception as e:
            log(f"Error sending phone call notification: {e}")


def send_whatsapp_notification(target):
    resource_type = notification_resource_type(target)
    prefs = user_preferences(target, "whatsapp", resource_type)
    if not prefs:
        return

    sending = False
    if prefs[resource_type]['whatsapp'].get("on_shift", False):
        current_shift = (
            Shift.query.join(ShiftUser)
            .filter(ShiftUser.user_id == target["user"]["id"])
            .filter(Shift.start_date <= arrow.utcnow().datetime)
            .filter(Shift.end_date >= arrow.utcnow().datetime)
            .first()
        )
        if current_shift is not None:
            sending = True

    timeslot = prefs[resource_type]['whatsapp'].get("time_slot", [])
    if len(timeslot) > 0:
        current_time = arrow.utcnow().datetime
        if timeslot[0] < timeslot[1]:
            if current_time.hour >= timeslot[0] and current_time.hour <= timeslot[1]:
                sending = True
        else:
            if current_time.hour <= timeslot[1] or current_time.hour >= timeslot[0]:
                sending = True

    if sending:
        try:
            client.messages.create(
                body=f'{cfg["app.title"]} - {target["text"]}',
                from_="whatsapp:" + str(from_number),
                to="whatsapp" + str(target["user"]["contact_phone"].e164),
            )
            log(
                f'Sent WhatsApp notification to user {target["user"]["id"]} at phone number: {target["user"]["contact_phone"].e164}, body: {target["text"]}, resource_type: {resource_type}'
            )
        except Exception as e:
            log(f"Error sending WhatsApp notification: {e}")


def push_frontend_notification(target):
    if 'user_id' in target:
        user_id = target["user_id"]
    elif 'user' in target:
        if 'id' in target["user"]:
            user_id = target["user"]["id"]
        else:
            user_id = None
    else:
        user_id = None

    if user_id is None:
        log(
            "Error sending frontend notification: user_id or user.id not found in notification's target"
        )
        return
    resource_type = notification_resource_type(target)
    log(
        f'Sent frontend notification to user {user_id}, body: {target["text"]}, resource_type: {resource_type}'
    )
    ws_flow = Flow()
    ws_flow.push(user_id, "skyportal/FETCH_NOTIFICATIONS")


def users_on_shift(session):
    users = session.scalars(
        sa.select(ShiftUser).where(
            ShiftUser.shift_id == Shift.id,
        )
    ).all()
    return [user.user_id for user in users]


def process_gcn_notification(data):
    target_class_name = data['target_class_name']
    target_id = data['target_id']
    target_content = None

    with DBSession() as session:
        stmt = sa.select(User).where(
            User.preferences["notifications"]["gcn_events"]["active"]
            .astext.cast(sa.Boolean)
            .is_(True),
        )
        if target_class_name == "GcnTag":
            stmt = stmt.where(
                User.preferences["notifications"]["gcn_events"]["new_tags"]
                .astext.cast(sa.Boolean)
                .is_(True),
            )
        users = session.scalars(stmt).all()

        if len(users) == 0:
            return []

        if target_class_name == "GcnTag":
            gcn_tag = session.scalars(
                sa.select(GcnTag).where(GcnTag.id == target_id)
            ).first()
            gcn_event = session.scalars(
                sa.select(GcnEvent).where(GcnEvent.dateobs == gcn_tag.dateobs)
            ).first()
            if len(gcn_event.localizations) > 0:
                target_id = gcn_event.localizations[0].id
            else:
                return

        target_class = GcnNotice if target_class_name == "GcnNotice" else Localization
        target = session.scalars(
            sa.select(target_class).where(target_class.id == target_id)
        ).first()
        target_data = target.to_dict()
        target_content = gcn_notification_content(target, session)

        event = session.scalars(
            sa.select(GcnEvent).where(GcnEvent.dateobs == target_data["dateobs"])
        ).first()
        notices = event.gcn_notices
        filtered_notices = [
            notice
            for notice in notices
            if (target_class_name == "GcnNotice" and notice.id == target_id)
            or (
                target_class_name != "GcnNotice"
                and notice.id == target_data["notice_id"]
            )
        ]
        if len(filtered_notices) == 0:
            return []
        notice = filtered_notices[0]

        notifications = []
        for user in users:
            gcn_prefs = (
                user.preferences.get('notifications', {})
                .get("gcn_events", {})
                .get("properties", {})
            )
            if len(gcn_prefs.keys()) == 0:
                continue

            # GCN notifications work with profiles
            # where a user can create different profiles, each with different notification settings
            for gcn_pref in gcn_prefs.values():
                if len(gcn_pref.get("gcn_notice_types", [])) > 0:
                    if (
                        not gcn.NoticeType(notice.notice_type).name
                        in gcn_pref['gcn_notice_types']
                    ):
                        continue

                if len(gcn_pref.get("gcn_tags", [])) > 0:
                    intersection = list(set(event.tags) & set(gcn_pref["gcn_tags"]))
                    if len(intersection) == 0:
                        continue

                if len(gcn_pref.get("gcn_properties", [])) > 0:
                    properties_bool = []
                    for properties in event.properties:
                        properties_dict = properties.data
                        properties_pass = True
                        for prop_filt in gcn_pref["gcn_properties"]:
                            prop_split = prop_filt.split(":")
                            if not len(prop_split) == 3:
                                raise ValueError(
                                    "Invalid propertiesFilter value -- property filter must have 3 values"
                                )
                            name = prop_split[0].strip()
                            if name in properties_dict:
                                value = prop_split[1].strip()
                                try:
                                    value = float(value)
                                except ValueError as e:
                                    raise ValueError(
                                        f"Invalid propertiesFilter value: {e}"
                                    )
                                op = prop_split[2].strip()
                                if op not in op_options:
                                    raise ValueError(f"Invalid operator: {op}")
                                comp_function = getattr(operator, op)
                                if not comp_function(properties_dict[name], value):
                                    properties_pass = False
                                    break
                        properties_bool.append(properties_pass)
                    if not any(properties_bool):
                        continue

                if target_class_name != "GcnNotice":
                    localization = session.scalars(
                        sa.select(Localization).where(Localization.id == target_id)
                    ).first()
                    (
                        localization_properties_dict,
                        localization_tags_list,
                    ) = get_skymap_properties(localization)

                    if len(gcn_pref.get("localization_tags", [])) > 0:
                        intersection = list(
                            set(localization_tags_list)
                            & set(gcn_pref["localization_tags"])
                        )
                        if len(intersection) == 0:
                            continue

                    for prop_filt in gcn_pref.get("localization_properties", []):
                        prop_split = prop_filt.split(":")
                        if not len(prop_split) == 3:
                            raise ValueError(
                                "Invalid propertiesFilter value -- property filter must have 3 values"
                            )
                        name = prop_split[0].strip()
                        if name in localization_properties_dict:
                            value = prop_split[1].strip()
                            try:
                                value = float(value)
                            except ValueError as e:
                                raise ValueError(f"Invalid propertiesFilter value: {e}")
                            op = prop_split[2].strip()
                            if op not in op_options:
                                raise ValueError(f"Invalid operator: {op}")
                            comp_function = getattr(operator, op)
                            if not comp_function(
                                localization_properties_dict[name],
                                value,
                            ):
                                continue

                if target_class_name == "GcnTag":
                    text = (
                        f"Updated GCN Event *{target_data['dateobs']}*, "
                        f"with Tag *{gcn_tag.text}*"
                    )
                else:
                    if len(notices) > 1:
                        text = (
                            f"New Notice for GCN Event *{target_data['dateobs']}*, "
                            f"with Notice Type *{gcn.NoticeType(notice.notice_type).name}*"
                        )
                    else:
                        text = (
                            f"New GCN Event *{target_data['dateobs']}*, "
                            f"with Notice Type *{gcn.NoticeType(notice.notice_type).name}*"
                        )

                notification = UserNotification(
                    user=user,
                    text=text,
                    notification_type="gcn_events_new_tag"
                    if target_class_name == "GcnTag"
                    else "gcn_events",
                    url=f"/gcn_events/{str(target_data['dateobs']).replace(' ','T')}",
                )
                session.add(notification)
                session.commit()
                target = {
                    **notification.to_dict(),
                    "user": {
                        **notification.user.to_dict(),
                        "preferences": notification.user.preferences,
                    },
                    "content": target_content,
                }
                notifications.append(target)

    return notifications


def process_followup_request_notification(data):
    target_class_name = data['target_class_name']
    target_id = data['target_id']
    target_allocation_id = data['allocation_id']
    target_obj_id = data['obj_id']
    target_content = None

    notifications = []
    with DBSession() as session:
        users = session.scalars(
            sa.select(User).where(
                User.preferences["notifications"]["followup_requests"]["active"]
                .astext.cast(sa.Boolean)
                .is_(True),
            )
        ).all()
        if len(users) == 0:
            return []

        followup_request = session.scalars(
            sa.select(FollowupRequest).where(FollowupRequest.id == target_id)
        ).first()

        if (
            not followup_request
            and target_allocation_id
            and target_obj_id
            or followup_request.status == "deleted"
        ):
            for user in users:
                allocation = session.scalar(
                    Allocation.select(user, mode="read").where(
                        Allocation.id
                        == (
                            target_allocation_id
                            if not followup_request
                            else followup_request.allocation_id
                        )
                    )
                )
                if allocation:
                    notification = UserNotification(
                        user=user,
                        text=f"A follow-up request for {target_obj_id} with allocation {allocation.id} was deleted",
                        notification_type="followup_requests",
                        url="/followup_requests",
                    )
                    session.add(notification)
                    session.commit()
                    target = {
                        **notification.to_dict(),
                        "user": {
                            **notification.user.to_dict(),
                            "preferences": notification.user.preferences,
                        },
                        "content": target_content,
                    }
                    notifications.append(target)
        else:
            for user in users:
                allocation = session.scalar(
                    Allocation.select(user, mode="read").where(
                        Allocation.id == target_allocation_id
                    )
                )
                if allocation:
                    notification = UserNotification(
                        user=user,
                        text=f"New follow-up request for {target_obj_id} with allocation {allocation.id}",
                        notification_type="followup_requests",
                        url=f"/followup_requests/{target_id}",
                    )
                    session.add(notification)
                    session.commit()
                    target = {
                        **notification.to_dict(),
                        "user": {
                            **notification.user.to_dict(),
                            "preferences": notification.user.preferences,
                        },
                        "content": target_content,
                    }
                    notifications.append(target)

    return notifications


def process_observation_plan_notification(data):
    target_class_name = data['target_class_name']
    target_id = data['target_id']
    target_content = None

    notifications = []
    with DBSession() as session:
        users = session.scalars(
            sa.select(User).where(
                User.preferences["notifications"]["observation_plans"]["active"]
                .astext.cast(sa.Boolean)
                .is_(True),
            )
        ).all()
        if len(users) == 0:
            return []

        observation_plan = session.scalars(
            sa.select(EventObservationPlan).where(EventObservationPlan.id == target_id)
        ).first()

        if observation_plan:
            for user in users:
                notification = UserNotification(
                    user=user,
                    text=f"New observation plan for GCN event {observation_plan.dateobs}",
                    notification_type="observation_plans",
                    url=f"/gcn_events/{str(observation_plan.dateobs).replace(' ','T')}",
                )
                session.add(notification)
                session.commit()
                target = {
                    **notification.to_dict(),
                    "user": {
                        **notification.user.to_dict(),
                        "preferences": notification.user.preferences,
                    },
                    "content": target_content,
                }
                notifications.append(target)

    return notifications


def process_comment_notification(data):
    target_id = data['target_id']
    target_content = None

    notifications = []
    with DBSession() as session:
        comment = session.scalar(sa.select(Comment).where(Comment.id == target_id))

        stmt = sa.select(User).where(
            User.preferences["notifications"]["favorite_sources"]["active"]
            .astext.cast(sa.Boolean)
            .is_(True),
            User.preferences["notifications"]["favorite_sources"]["new_comments"]
            .astext.cast(sa.Boolean)
            .is_(True),
        )
        if comment.bot:
            stmt = stmt.where(
                User.preferences["notifications"]["favorite_sources"][
                    "new_bot_comments"
                ]
                .astext.cast(sa.Boolean)
                .is_(True)
            )

        stmt = stmt.where(
            User.id.in_(
                sa.select(Listing.user_id).where(
                    Listing.obj_id == comment.obj_id,
                    Listing.list_name == "favorites",
                )
            )
        )

        users = session.scalars(stmt).all()
        if len(users) == 0:
            return []

        for user in users:
            notification = UserNotification(
                user=user,
                text=f"New comment on favorite source {comment.obj_id}",
                notification_type="favorite_sources",
                url=f"/source/{comment.obj_id}",
            )
            session.add(notification)
            session.commit()
            target = {
                **notification.to_dict(),
                "user": {
                    **notification.user.to_dict(),
                    "preferences": notification.user.preferences,
                },
                "content": target_content,
            }
            notifications.append(target)

    return notifications


def process_classification_notification(data):
    target_id = data['target_id']
    target_content = None

    notifications = []
    with DBSession() as session:
        classification = session.scalar(
            sa.select(Classification).where(Classification.id == target_id)
        )

        stmt = sa.select(User).where(
            User.preferences["notifications"]["favorite_sources"]["active"]
            .astext.cast(sa.Boolean)
            .is_(True),
            User.preferences["notifications"]["favorite_sources"]["new_classifications"]
            .astext.cast(sa.Boolean)
            .is_(True),
        )

        stmt = stmt.where(
            User.id.in_(
                sa.select(Listing.user_id).where(
                    Listing.obj_id == classification.obj_id,
                    Listing.list_name == "favorites",
                )
            )
        )

        users = session.scalars(stmt).all()
        if len(users) == 0:
            return []

        for user in users:
            notification = UserNotification(
                user=user,
                text=f"New classification on favorite source {classification.obj_id}",
                notification_type="favorite_sources",
                url=f"/source/{classification.obj_id}",
            )
            session.add(notification)
            session.commit()
            target = {
                **notification.to_dict(),
                "user": {
                    **notification.user.to_dict(),
                    "preferences": notification.user.preferences,
                },
                "content": target_content,
            }
            notifications.append(target)

    return notifications


def process_spectra_notification(data):
    target_id = data['target_id']
    target_content = None

    notifications = []
    with DBSession() as session:
        spectra = session.scalar(sa.select(Spectrum).where(Spectrum.id == target_id))

        # first we notify users who have favorited the source and have spectra notifications enabled
        stmt = sa.select(User).where(
            User.preferences["notifications"]["favorite_sources"]["active"]
            .astext.cast(sa.Boolean)
            .is_(True),
            User.preferences["notifications"]["favorite_sources"]["new_spectra"]
            .astext.cast(sa.Boolean)
            .is_(True),
        )

        stmt = stmt.where(
            User.id.in_(
                sa.select(Listing.user_id).where(
                    Listing.obj_id == spectra.obj_id,
                    Listing.list_name == "favorites",
                )
            )
        )

        users = session.scalars(stmt).all()
        for user in users:
            notification = UserNotification(
                user=user,
                text=f"New spectrum on favorite source {spectra.obj_id}",
                notification_type="favorite_sources",
                url=f"/source/{spectra.obj_id}",
            )
            session.add(notification)
            session.commit()
            target = {
                **notification.to_dict(),
                "user": {
                    **notification.user.to_dict(),
                    "preferences": notification.user.preferences,
                },
                "content": target_content,
            }
            notifications.append(target)

        # then we notify users other users, that have sources notifications activated (not just favorite_sources)
        # and that want to be notification on new spectrum
        stmt = sa.select(User).where(
            User.preferences["notifications"]["sources"]["active"].astext.cast(
                sa.Boolean
            ),
            User.preferences["notifications"]["sources"]["new_spectra"].astext.cast(
                sa.Boolean
            ),
            User.id.notin_([u.id for u in users]),
        )

        users = session.scalars(stmt).all()
        for user in users:
            notification = UserNotification(
                user=user,
                text=f"New spectrum on source {spectra.obj_id}",
                notification_type="sources",
                url=f"/source/{spectra.obj_id}",
            )
            session.add(notification)
            session.commit()
            target = {
                **notification.to_dict(),
                "user": {
                    **notification.user.to_dict(),
                    "preferences": notification.user.preferences,
                },
                "content": target_content,
            }
            notifications.append(target)

    return notifications


def process_group_admission_request_notification(data):
    target_id = data['target_id']
    target_content = None

    notifications = []
    with DBSession() as session:
        group_admission_request = session.scalar(
            sa.select(GroupAdmissionRequest).where(
                GroupAdmissionRequest.id == target_id
            )
        )
        # find admins of the group
        users = session.scalars(
            sa.select(User).where(
                User.id.in_(
                    sa.select(GroupUser.user_id).where(
                        GroupUser.group_id == group_admission_request.group_id,
                        GroupUser.admin.is_(True),
                    )
                )
            )
        ).all()

        for user in users:
            notification = UserNotification(
                user=user,
                text=f"User {group_admission_request.user.username} requested to join group {group_admission_request.group.name}",
                notification_type="group_admission_requests",
                url=f"/group/{group_admission_request.group_id}",
            )
            session.add(notification)
            session.commit()
            target = {
                **notification.to_dict(),
                "user": {
                    **notification.user.to_dict(),
                    "preferences": notification.user.preferences,
                },
                "content": target_content,
            }
            notifications.append(target)

    return notifications


def process_notifications(data):
    target_class_name = data['target_class_name']

    is_group_admission_request = target_class_name == "GroupAdmissionRequest"
    is_analysis_service = target_class_name == "ObjAnalysis"

    print(f"PROCESS: {data}")

    if target_class_name in ["GcnNotice", "Localization", "GcnTag"]:
        return process_gcn_notification(data)
    elif target_class_name == "FollowupRequest":
        return process_followup_request_notification(data)
    elif target_class_name == "EventObservationPlan":
        return process_observation_plan_notification(data)
    elif target_class_name == "Comment":
        return process_comment_notification(data)
    elif target_class_name == "Classification":
        return process_classification_notification(data)
    elif target_class_name == "GroupAdmissionRequest":
        return process_group_admission_request_notification(data)


# here we send the notifications to the individual users that need to be notified
def send_notifications(notifications_queue):
    while True:
        if len(notifications_queue) == 0:
            time.sleep(1)
            continue

        notification = notifications_queue.pop(0)
        if not isinstance(notification, dict):
            continue

        try:
            push_frontend_notification(notification)
            send_phone_notification(notification)
            send_sms_notification(notification)
            send_whatsapp_notification(notification)
            send_email_notification(notification)
            send_slack_notification(notification)
        except Exception as e:
            log(f"Error processing notification ID {notification['id']}: {str(e)}")


# here we process the DB triggers, figuring out which users need to be notified
def generate_notifications(notifications_candidates_queue, notifications_queue):
    while True:
        if len(notifications_candidates_queue) == 0:
            time.sleep(1)
            continue
        notifications = notifications_candidates_queue.pop(0)
        if not isinstance(notifications, dict):
            continue

        print(f"GENERATE: {notifications}")

        notifications = process_notifications(notifications)
        if not isinstance(notifications, list) or len(notifications) == 0:
            continue

        for notification in notifications:
            notifications_queue.append(notification)


# The API gets the DB triggers from the app, and adds them to the queue of notifications candidates
def api(notifications_candidates_queue):
    class QueueHandler(tornado.web.RequestHandler):
        def get(self):
            self.set_header("Content-Type", "application/json")
            self.write(
                {
                    "status": "success",
                    "data": {"queue_length": len(notifications_candidates_queue)},
                }
            )

        async def post(self):
            try:
                data = tornado.escape.json_decode(self.request.body)
            except json.JSONDecodeError:
                self.set_status(400)
                return self.write({"status": "error", "message": "Malformed JSON data"})

            print(f"API: {data}")

            notifications_candidates_queue.append(data)

            self.set_status(200)
            self.write({"status": "success", "message": "Notification added to queue"})
            return

    app = tornado.web.Application([(r"/", QueueHandler)])
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    log(f"API: {cfg['ports.notification_queue']}")
    app.listen(cfg["ports.notification_queue"])
    loop.run_forever()


notifications_candidates_queue = []
notifications_queue = []

if __name__ == "__main__":
    try:
        t = Thread(target=send_notifications, args=(notifications_queue,))
        t2 = Thread(
            target=generate_notifications,
            args=(notifications_candidates_queue, notifications_queue),
        )
        t3 = Thread(target=api, args=(notifications_candidates_queue,))
        t.start()
        t2.start()
        t3.start()

        while True:
            log(
                f"Current notification candidates queue length: {len(notifications_candidates_queue)}"
            )
            log(f"Current notification queue length: {len(notifications_queue)}")
            time.sleep(10)
            if not t.is_alive():
                log("User Notification queue thread died, restarting")
                t = Thread(target=send_notifications, args=(notifications_queue,))
                t.start()
            if not t2.is_alive():
                log("Notification generation thread died, restarting")
                t2 = Thread(
                    target=generate_notifications,
                    args=(notifications_candidates_queue, notifications_queue),
                )
                t2.start()
            if not t3.is_alive():
                log("API thread died, restarting")
                t3 = Thread(target=api, args=(notifications_candidates_queue,))
                t3.start()
    except Exception as e:
        log(f"Error starting notification queue: {str(e)}")
        raise e
