"""gcn_allocation_triggers

Revision ID: 05c1ca1487a9
Revises: e268e17ca352
Create Date: 2023-03-08 13:21:13.704126

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '05c1ca1487a9'
down_revision = 'e268e17ca352'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table(
        'gcntriggers',
        sa.Column('dateobs', sa.DateTime(), nullable=False),
        sa.Column('allocation_id', sa.Integer(), nullable=False),
        sa.Column('triggered', sa.Boolean(), nullable=False),
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.Column('modified', sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(
            ['allocation_id'], ['allocations.id'], ondelete='CASCADE'
        ),
        sa.ForeignKeyConstraint(['dateobs'], ['gcnevents.dateobs'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('allocation_id', 'id'),
    )
    op.create_index(
        op.f('ix_gcntriggers_created_at'), 'gcntriggers', ['created_at'], unique=False
    )
    op.create_index(
        op.f('ix_gcntriggers_dateobs'), 'gcntriggers', ['dateobs'], unique=False
    )
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index(op.f('ix_gcntriggers_dateobs'), table_name='gcntriggers')
    op.drop_index(op.f('ix_gcntriggers_created_at'), table_name='gcntriggers')
    op.drop_table('gcntriggers')
    # ### end Alembic commands ###