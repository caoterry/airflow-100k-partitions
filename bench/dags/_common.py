"""Shared helpers for the 100k-partition benchmark DAGs (Airflow 3.3.x)."""
from __future__ import annotations

from airflow.providers.standard.operators.empty import EmptyOperator

DEFAULT_N = 1000


def account_ids(n: int, prefix: str = "ACCT") -> list[str]:
    """Deterministic pseudo firm-account ids, 12 chars each: ACCT00000001 ..."""
    return [f"{prefix}{i:08d}" for i in range(1, n + 1)]


def chunks(items: list, size: int) -> list[list]:
    return [items[i : i + size] for i in range(0, len(items), size)]


class NoopAccountOperator(EmptyOperator):
    """An EmptyOperator that accepts a mapped `account` argument.

    EmptyOperator instances are short-circuited by the scheduler (marked success without being sent
    to the executor) as long as they have no callbacks/inlets/outlets, so a mapped NoopAccountOperator
    lets us benchmark pure scheduler + metadata-DB cost of N task instances.
    """

    def __init__(self, account: str, **kwargs):
        super().__init__(**kwargs)
        self.account = account
