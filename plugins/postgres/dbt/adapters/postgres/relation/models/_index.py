from dataclasses import dataclass, field
from typing import Any, Dict, FrozenSet, Set

import agate

from dbt.adapters.relation.models import (
    RelationChangeAction,
    RelationChange,
    RelationComponent,
)
from dbt.adapters.validation import ValidationMixin, ValidationRule
from dbt.dataclass_schema import StrEnum
from dbt.exceptions import DbtRuntimeError


class PostgresIndexMethod(StrEnum):
    btree = "btree"
    hash = "hash"
    gist = "gist"
    spgist = "spgist"
    gin = "gin"
    brin = "brin"

    @classmethod
    def default(cls) -> "PostgresIndexMethod":
        return cls.btree


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class PostgresIndexRelation(RelationComponent, ValidationMixin):
    """
    This config fallows the specs found here:
    https://www.postgresql.org/docs/current/sql-createindex.html

    The following parameters are configurable by dbt:
    - name: the name of the index in the database, this isn't predictable since we apply a timestamp
    - unique: checks for duplicate values when the index is created and on data updates
    - method: the index method to be used
    - column_names: the columns in the index

    Applicable defaults for non-configurable parameters:
    - concurrently: `False`
    - nulls_distinct: `True`
    """

    name: str = field(default=None, hash=False, compare=False)
    column_names: FrozenSet[str] = field(default_factory=frozenset, hash=True)
    unique: bool = field(default=False, hash=True)
    method: PostgresIndexMethod = field(default=PostgresIndexMethod.default(), hash=True)

    @property
    def validation_rules(self) -> Set[ValidationRule]:
        return {
            ValidationRule(
                validation_check=self.column_names is not None,
                validation_error=DbtRuntimeError(
                    "Indexes require at least one column, but none were provided"
                ),
            ),
        }

    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> "PostgresIndexRelation":
        # TODO: include the QuotePolicy instead of defaulting to lower()
        kwargs_dict = {
            "name": config_dict.get("name"),
            "column_names": frozenset(
                column.lower() for column in config_dict.get("column_names", set())
            ),
            "unique": config_dict.get("unique"),
            "method": config_dict.get("method"),
        }
        index = super().from_dict(kwargs_dict)
        assert isinstance(index, PostgresIndexRelation)
        return index

    @classmethod
    def parse_node(cls, node: Dict[str, Any]) -> Dict[str, Any]:
        config_dict = {
            "column_names": set(node.get("columns", set())),
            "unique": node.get("unique"),
            "method": node.get("type"),
        }
        return config_dict

    @classmethod
    def parse_describe_relation_results(
        cls, describe_relation_results: agate.Row
    ) -> Dict[str, Any]:
        config_dict = {
            "name": describe_relation_results.get("name"),
            "column_names": set(describe_relation_results.get("column_names", "").split(",")),
            "unique": describe_relation_results.get("unique"),
            "method": describe_relation_results.get("method"),
        }
        return config_dict

    @property
    def as_node_config(self) -> Dict[str, Any]:
        """
        Returns: a dictionary that can be passed into `get_create_index_sql()`
        """
        node_config = {
            "columns": list(self.column_names),
            "unique": self.unique,
            "type": self.method.value,
        }
        return node_config


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class PostgresIndexRelationChange(RelationChange, ValidationMixin):
    """
    Example of an index change:
    {
        "action": "create",
        "context": {
            "name": "",  # we don't know the name since it gets created as a hash at runtime
            "columns": ["column_1", "column_3"],
            "type": "hash",
            "unique": True
        }
    },
    {
        "action": "drop",
        "context": {
            "name": "index_abc",  # we only need this to drop, but we need the rest to compare
            "columns": ["column_1"],
            "type": "btree",
            "unique": True
        }
    }
    """

    context: PostgresIndexRelation

    @property
    def requires_full_refresh(self) -> bool:
        return False

    @property
    def validation_rules(self) -> Set[ValidationRule]:
        return {
            ValidationRule(
                validation_check=self.action
                in {RelationChangeAction.create, RelationChangeAction.drop},
                validation_error=DbtRuntimeError(
                    "Invalid operation, only `drop` and `create` changes are supported for indexes."
                ),
            ),
            ValidationRule(
                validation_check=not (
                    self.action == RelationChangeAction.drop and self.context.name is None
                ),
                validation_error=DbtRuntimeError(
                    "Invalid operation, attempting to drop an index with no name."
                ),
            ),
            ValidationRule(
                validation_check=not (
                    self.action == RelationChangeAction.create
                    and self.context.column_names == set()
                ),
                validation_error=DbtRuntimeError(
                    "Invalid operations, attempting to create an index with no columns."
                ),
            ),
        }