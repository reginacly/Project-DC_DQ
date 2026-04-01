from great_expectations.expectations.metrics import (
    ColumnMapMetricProvider,
    column_condition_partial,
)
from great_expectations.expectations.expectation import ColumnMapExpectation
from great_expectations.execution_engine import SqlAlchemyExecutionEngine, PandasExecutionEngine
from sqlalchemy import func, and_
import pandas
import re

class ColumnValuesToMatchIndonesianPhoneRules(
    ColumnMapMetricProvider
):
    condition_metric_name = "column_values.match_indonesian_phone_rules"

    @column_condition_partial(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(cls, column, **kwargs):
        # return f"""
        #     LEN({column}) >= 8
        # """
        return and_(
            func.length(column)>=8,
            func.length(column)<=15,
            column.op("~")("^(?:\+62|62|0)8[0-9]{7,13}$")
        )
    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs):

        # convert ke string + handle null
        col = column.astype(str).fillna("")

        pattern = r"^(?:\+62|62|0)8[0-9]{7,13}$"

        return (
            col.str.len().between(8, 15)
            & col.str.match(pattern)
        )
    
class ExpectColumnValuesToMatchIndonesianPhoneRules(
    ColumnMapExpectation
):
    map_metric  = "column_values.match_indonesian_phone_rules"

    success_keys = ("mostly",)

    default_kwarg_values = {"mostly" : 1.0}