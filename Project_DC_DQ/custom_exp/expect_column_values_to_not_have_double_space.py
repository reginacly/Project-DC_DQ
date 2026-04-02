from __future__ import annotations

from typing import Optional

from great_expectations.expectations.expectation import ColumnMapExpectation
from great_expectations.expectations.metrics.map_metric_provider import (
    ColumnMapMetricProvider,
    column_condition_partial,
)
from great_expectations.execution_engine import (
    PandasExecutionEngine,
    SparkDFExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.expectation_configuration import ExpectationConfiguration
from great_expectations.expectations.registry import register_expectation


# ============================================================
# METRIC
# ============================================================
class ColumnValuesNotHaveDoubleSpace(ColumnMapMetricProvider):
    """
    Valid = does NOT contain multiple consecutive spaces
    """

    condition_metric_name = "column_values.not_have_double_space"

    # ======================
    # PANDAS (ODPS path)
    # ======================
    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs):

        # handle null safely
        col = column.fillna("").astype(str)

        # True = valid
        return ~col.str.contains(r"\s{2,}", regex=True)


    # ======================
    # SPARK (optional)
    # ======================
    @column_condition_partial(engine=SparkDFExecutionEngine)
    def _spark(cls, column, **kwargs):
        return ~column.rlike(r"\s{2,}")


    # ======================
    # SQL (Holo / Trino / DWH / ODPS SQLAlchemy)
    # ======================
    @column_condition_partial(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(cls, column, **kwargs):

        # handle NULL → treat as valid (optional)
        return (column == None) | (~column.like("%  %"))


# ============================================================
# EXPECTATION
# ============================================================
class ExpectColumnValuesToNotHaveDoubleSpace(ColumnMapExpectation):
    """
    Expect column values to NOT contain double spaces
    """

    map_metric = "column_values.not_have_double_space"

    success_keys = ("mostly",)

    default_kwarg_values = {
        "mostly": 1.0,
        "result_format": "BASIC",
        "row_condition": None,
        "condition_parser": None,
        "include_config": True,
        "catch_exceptions": False,
    }

    def validate_configuration(
        self,
        configuration: Optional[ExpectationConfiguration],
    ) -> None:
        super().validate_configuration(configuration)


# ============================================================
# REGISTER
# ============================================================
register_expectation(ExpectColumnValuesToNotHaveDoubleSpace)