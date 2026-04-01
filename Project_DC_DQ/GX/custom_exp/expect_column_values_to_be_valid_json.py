import json
import great_expectations as gx
import sqlalchemy as sa

from great_expectations.execution_engine import (
    PandasExecutionEngine,
    SqlAlchemyExecutionEngine,
)

from great_expectations.expectations.metrics import (
    ColumnMapMetricProvider,
    column_condition_partial,
)

from great_expectations.expectations.expectation import ColumnMapExpectation


# =========================================
# 1. CUSTOM METRIC (JSON VALIDATION)
# =========================================
class ColumnValuesToBeValidJson(ColumnMapMetricProvider):
    condition_metric_name = "column_values.valid_json"

    # -----------------------
    # Pandas Engine
    # -----------------------
    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs):
        def is_valid_json(val):
            if val is None:
                return True  # null handled separately
            try:
                json.loads(val)
                return True
            except Exception:
                return False

        return column.apply(is_valid_json)

    # -----------------------
    # SQL Engine (Trino, MSSQL, Postgres)
    # -----------------------
    @column_condition_partial(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(cls, column, **kwargs):
        engine = cls.execution_engine.engine.dialect.name.lower()

        if "trino" in engine:
            return sa.text(f"try(json_parse({column.name})) IS NOT NULL")

        if "mssql" in engine:
            return sa.func.isjson(column) == 1

        if "postgresql" in engine:
            return sa.text(f"{column.name}::json IS NOT NULL")

        raise NotImplementedError(f"JSON validation not supported for {engine}")


# =========================================
# 2. EXPECTATION CLASS
# =========================================
class ExpectColumnValuesToBeValidJson(ColumnMapExpectation):
    map_metric = "column_values.valid_json"
    success_keys = ("mostly",)
    default_kwarg_values = {"mostly": 1.0}