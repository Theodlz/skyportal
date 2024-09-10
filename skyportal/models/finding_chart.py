__all__ = ['ObjFindingChart']

import hashlib
import json
import os

import conesearch_alchemy
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

from baselayer.app.env import load_env
from baselayer.app.models import (
    Base,
)

_, cfg = load_env()

MAX_FILEPATH_LENGTH = 255


class ObjFindingChart(Base, conesearch_alchemy.Point):
    obj_id = sa.Column(
        sa.ForeignKey('objs.id', ondelete='CASCADE'),
        primary_key=True,
        doc='ID of the object',
        index=True,
    )

    ra = sa.Column(
        sa.Float,
        nullable=False,
        doc='Right ascension used to generate the finding chart',
    )

    dec = sa.Column(
        sa.Float,
        nullable=False,
        doc='Declination used to generate the finding chart',
    )

    facility = sa.Column(
        sa.String,
        doc='Facility name',
        index=True,
    )

    obstime = sa.Column(
        sa.DateTime,
        nullable=False,
        doc='Observation timestamp',
        index=True,
    )

    params = sa.Column(
        JSONB,
        nullable=False,
        doc='Parameters used to generate the finding chart',
    )

    params_hash = sa.Column(
        sa.String,
        nullable=False,
        doc='Hash of the parameters used to generate the finding chart, to check for duplicates',
    )

    _path = sa.Column(
        sa.String,
        nullable=True,
        doc='Path to the finding chart file stored on disk',
    )

    def filename(self):
        if not self.params_hash:
            self.calc_hash()
        return f'{self.obj_id}_{self.facility}_{self.params_hash}.pdf'

    def calc_hash(self):
        self.params_hash = hashlib.sha256(
            json.dumps(self.params, sort_keys=True).encode()
        ).hexdigest()

    def save_data(self, data):
        root_folder = cfg.get('finding_charts.folder', 'finding_charts')

        filename = self.filename()
        path = os.path.join(root_folder, self.obj_id)
        if not os.path.exists(path):
            os.makedirs(path)

        full_name = os.path.join(path, filename)

        if len(full_name) > MAX_FILEPATH_LENGTH:
            raise ValueError(
                f'Full path to file {full_name} is longer than {MAX_FILEPATH_LENGTH} characters.'
            )

        # data is a matplotlib image we exported to PDF as a BytesIO object
        with open(full_name, 'wb') as f:
            f.write(data)

        self._path = full_name

    def data(self, output_format='pdf'):
        if not self._path:
            return None

        content = None
        with open(self._path, 'rb') as f:
            content = f.read()

        if output_format == 'pdf':
            return content
        elif output_format == 'png':
            # convert PDF to PNG
            import pdf2image
            from PIL import Image  # noqa

            images = pdf2image.convert_from_bytes(content)
            img = images[0]
            img.save('temp.png', 'PNG')
            with open('temp.png', 'rb') as f:
                return f.read()
