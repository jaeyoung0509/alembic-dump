# tests/alembic_test_env/versions/0001_create_users_table.py
"""create users table

Revision ID: 0001
Revises:
Create Date: 2025-05-10 16:00:00.000000 # adjust to appropriate current date/time

"""

from collections.abc import Sequence
from typing import Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Creates the users table with basic fields."""
    op.create_table(
        "users",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("name", sa.String(50)),
        sa.Column("email", sa.String(100)),
    )


def downgrade() -> None:
    """Removes the users table."""
    op.drop_table("users")
