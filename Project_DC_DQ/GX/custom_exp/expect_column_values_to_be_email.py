from great_expectations.compatibility import sqlalchemy as sa
from great_expectations.execution_engine import SqlAlchemyExecutionEngine, PandasExecutionEngine
from great_expectations.expectations.metrics import (
    ColumnMapMetricProvider,
    column_condition_partial,
)
from great_expectations.expectations.expectation import ColumnMapExpectation


class ColumnValuesToBeEmail(ColumnMapMetricProvider):
    """
    Metric that evaluates to True if the column value looks like an email.
    Implemented only for SqlAlchemyExecutionEngine.
    """
    condition_metric_name = "column_values.to_be_email"
    condition_value_keys = ()  # no extra config for the metric itself

    @column_condition_partial(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(cls, column, **kwargs):
        # Simple, practical email regex (not 100% RFC-complete, but good for DQ)
        return column.op("~*")(
            r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
        )
    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs):

        col = column.astype(str).fillna("")

        pattern = r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'

        return col.str.match(pattern)

class ExpectColumnValuesToBeEmail(ColumnMapExpectation):
    """
    Expect column values to be valid-looking email addresses,
    using a regex evaluated in the SQL engine.
    """

    # Link to the metric defined above
    map_metric = "column_values.to_be_email"

    # You can use "mostly" like any other ColumnMapExpectation
    success_keys = ("mostly",)

    # Default kwargs (you can still override in suite config)
    default_kwarg_values = {
        "mostly": 1.0,
        "row_condition": None,
        "condition_parser": None,
    }

    # Optional: give it a unique name if you like
    library_metadata = {
        "maturity": "experimental",
        "package": "custom_expectations",
        "tags": ["format", "email", "sqlalchemy"],
    }

