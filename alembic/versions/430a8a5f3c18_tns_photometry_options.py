"""tns_photometry_options

Revision ID: 430a8a5f3c18
Revises: af970c6b6b3c
Create Date: 2024-03-22 15:47:45.018523

"""

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision = "430a8a5f3c18"
down_revision = "af970c6b6b3c"
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column(
        "tnsrobot_submissions",
        sa.Column(
            "photometry_options", postgresql.JSONB(astext_type=sa.Text()), nullable=True
        ),
    )
    op.add_column(
        "tnsrobots",
        sa.Column(
            "photometry_options", postgresql.JSONB(astext_type=sa.Text()), nullable=True
        ),
    )
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column("tnsrobots", "photometry_options")
    op.drop_column("tnsrobot_submissions", "photometry_options")
    # ### end Alembic commands ###
