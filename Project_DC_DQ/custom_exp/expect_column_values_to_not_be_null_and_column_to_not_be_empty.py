from __future__ import annotations

from typing import Dict, Optional

import pandas as pd
import sqlalchemy as sa

from great_expectations.core import ExpectationValidationResult
from great_expectations.core.metric_function_types import SummarizationMetricNameSuffixes
from great_expectations.execution_engine import SqlAlchemyExecutionEngine, PandasExecutionEngine
from great_expectations.execution_engine import ExecutionEngine
from great_expectations.expectations.expectation import (
    ColumnMapExpectation,
    _format_map_output,
)
from great_expectations.expectations.expectation_configuration import parse_result_format

# Metric provider imports
from great_expectations.expectations.metrics.map_metric_provider import (
    ColumnMapMetricProvider,
    column_condition_partial,
)

from great_expectations.execution_engine import SqlAlchemyExecutionEngine

class ColumnValuesNonNullAndNotWhitespace(ColumnMapMetricProvider):
    """
    Returns a boolean mask / SQL expression for:
      value IS NOT NULL AND length(trim(value)) > 0
    """

    condition_metric_name = "column_values.nonnull_and_not_whitespace"

    @column_condition_partial(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(cls, column, _dialect, **kwargs):
        # SQL portable form:
        #   column IS NOT NULL AND LENGTH(TRIM(column)) > 0
        #
        # Works for:
        # - Hologres (Postgres-like)
        # - Trino (via SQLAlchemy dialect, usually supports trim/length)
        # - ODPS (depends on dialect, but trim/length are common)
        #
        # If a dialect has different function names, you can branch on _dialect.name here.
        return sa.and_(
            column.isnot(None),
            sa.func.length(sa.func.trim(column)) > 0,
        )
    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs):

        col = column.astype(str)

        return (
            column.notna() &
            (col.str.strip().str.len() > 0)
        )

class ExpectColumnValuesToNotBeNullAndColumnToNotBeEmpty(ColumnMapExpectation):
    map_metric = "column_values.nonnull_and_not_whitespace"
    args_keys = ("column",)

    # Optional: keep mostly support (ColumnMapExpectation already supports it)
    mostly: Optional[float] = 1.0

    def _validate(
        self,
        metrics: Dict,
        runtime_configuration: Optional[dict] = None,
        execution_engine: Optional[ExecutionEngine] = None,
    ) -> ExpectationValidationResult:
        result_format = self._get_result_format(runtime_configuration=runtime_configuration)
        mostly = self._get_success_kwargs().get("mostly", self._get_default_value("mostly"))

        total_count = metrics.get("table.row_count")
        unexpected_count = metrics.get(
            f"{self.map_metric}.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}"
        )

        if total_count is None or total_count == 0:
            success = False
        else:
            success_ratio = (total_count - (unexpected_count or 0)) / total_count
            success = success_ratio >= (mostly if mostly is not None else 1.0)

        return _format_map_output(
            result_format=parse_result_format(result_format),
            success=success,
            element_count=total_count,
            unexpected_count=unexpected_count,
            unexpected_list=metrics.get(
                f"{self.map_metric}.{SummarizationMetricNameSuffixes.UNEXPECTED_VALUES.value}"
            ),
            unexpected_index_list=metrics.get(
                f"{self.map_metric}.{SummarizationMetricNameSuffixes.UNEXPECTED_INDEX_LIST.value}"
            ),
            unexpected_index_query=metrics.get(
                f"{self.map_metric}.{SummarizationMetricNameSuffixes.UNEXPECTED_INDEX_QUERY.value}"
            ),
        )

if __name__ == "__main__":
    ExpectColumnValuesToNotBeNullAndColumnToNotBeEmpty().print_diagnostic_checklist(
        show_failed_tests=True
    )