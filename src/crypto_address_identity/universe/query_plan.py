"""Immutable BigQuery SQL loading and identifier validation."""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from importlib import resources


_DATASET_RE = re.compile(
    r"^[a-z0-9][a-z0-9_-]{4,62}\.[A-Za-z_][A-Za-z0-9_]*$"
)


class InvalidBigQueryDataset(ValueError):
    """Raised before an unsafe identifier can enter SQL text."""


@dataclass(frozen=True)
class BigQueryQueryPlan:
    dataset: str
    outputs_table_id: str
    inputs_table_id: str
    address_features_sql: str
    source_checkpoint_sql: str
    address_features_sha256: str
    source_checkpoint_sha256: str

    @classmethod
    def load(cls, dataset: str) -> "BigQueryQueryPlan":
        if not _DATASET_RE.fullmatch(dataset):
            raise InvalidBigQueryDataset("BigQuery dataset identifier is invalid")
        package_root = resources.files("crypto_address_identity.universe")
        address_template = (
            package_root.joinpath("sql/bigquery/address_features.sql")
            .read_text(encoding="utf-8")
        )
        checkpoint_template = (
            package_root.joinpath("sql/bigquery/source_checkpoint.sql")
            .read_text(encoding="utf-8")
        )
        outputs_table_id = f"{dataset}.outputs"
        inputs_table_id = f"{dataset}.inputs"
        replacements = {
            "{{OUTPUTS_TABLE}}": f"`{outputs_table_id}`",
            "{{INPUTS_TABLE}}": f"`{inputs_table_id}`",
        }
        address_sql = address_template
        checkpoint_sql = checkpoint_template
        for marker, identifier in replacements.items():
            address_sql = address_sql.replace(marker, identifier)
            checkpoint_sql = checkpoint_sql.replace(marker, identifier)
        if "{{" in address_sql or "{{" in checkpoint_sql:
            raise InvalidBigQueryDataset("BigQuery SQL contains an unresolved marker")
        return cls(
            dataset=dataset,
            outputs_table_id=outputs_table_id,
            inputs_table_id=inputs_table_id,
            address_features_sql=address_sql,
            source_checkpoint_sql=checkpoint_sql,
            address_features_sha256=cls.hash_sql(address_sql),
            source_checkpoint_sha256=cls.hash_sql(checkpoint_sql),
        )

    @staticmethod
    def hash_sql(sql: str) -> str:
        return hashlib.sha256(sql.encode("utf-8")).hexdigest()
