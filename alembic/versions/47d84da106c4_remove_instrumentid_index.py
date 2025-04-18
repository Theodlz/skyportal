"""remove_instrumentid_index

Revision ID: 47d84da106c4
Revises: fdab0cc9eb78
Create Date: 2023-04-22 12:29:30.366383

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "47d84da106c4"
down_revision = "fdab0cc9eb78"
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index(
        "ix_instrumentfieldtiles_instrument_id", table_name="instrumentfieldtiles"
    )
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_index(
        "ix_instrumentfieldtiles_instrument_id",
        "instrumentfieldtiles",
        ["instrument_id"],
        unique=False,
    )
    # ### end Alembic commands ###
