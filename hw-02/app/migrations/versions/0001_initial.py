"""sink: processed_events, pod_counters

Revision ID: 0001
Revises:
"""
import sqlalchemy as sa
from alembic import op

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "processed_events",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("event_id", sa.Uuid(), nullable=False),
        sa.Column("user_id", sa.Text(), nullable=False),
        sa.Column("seq", sa.BigInteger(), nullable=False),
        sa.Column("type", sa.Text(), nullable=False),
        sa.Column("pod", sa.Text(), nullable=False),
        sa.Column("topic", sa.Text(), nullable=False),
        sa.Column("partition", sa.Integer(), nullable=False),
        sa.Column("offset", sa.BigInteger(), nullable=False),
        sa.Column(
            "processed_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("clock_timestamp()"),
            nullable=False,
        ),
        # Дедупликация: уникальность event_id гарантирует сама БД.
        sa.UniqueConstraint("event_id", name="processed_events_event_id_key"),
    )
    op.create_index("processed_events_user_idx", "processed_events", ["user_id", "id"])
    op.create_table(
        "pod_counters",
        sa.Column("pod", sa.Text(), primary_key=True),
        sa.Column("dedup_dropped", sa.BigInteger(), server_default=sa.text("0"), nullable=False),
    )


def downgrade() -> None:
    op.drop_table("pod_counters")
    op.drop_index("processed_events_user_idx", table_name="processed_events")
    op.drop_table("processed_events")
