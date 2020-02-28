#!/usr/bin/env python
# -*- coding: utf-8 -*-

__version__ = "0.0.1"

from pyspark.sql import DataFrame

# Add expectations to PySpark DataFrames
from .expectations import (
    expect_table_row_count_to_be_between,
    expect_column_values_to_be_null,
    expect_column_values_to_not_be_null,
    expect_column_values_to_be_between,
    expect_column_values_to_match_regex,
    expect_column_values_not_to_match_regex,
    expect_column_values_to_be_in_set,
    expect_column_values_to_be_unique
)

from .arguments_format_checker import (
    check_var_type,
    check_min_max,
    check_unexpected_percent,
    check_column_name,
    check_allowed_unexpected_percent
)


DataFrame.expect_table_row_count_to_be_between = expect_table_row_count_to_be_between
DataFrame.expect_column_values_to_be_null = expect_column_values_to_be_null
DataFrame.expect_column_values_to_not_be_null = expect_column_values_to_not_be_null
DataFrame.expect_column_values_to_be_between = expect_column_values_to_be_between
DataFrame.expect_column_values_to_match_regex = expect_column_values_to_match_regex
DataFrame.expect_column_values_not_to_match_regex = expect_column_values_not_to_match_regex
DataFrame.expect_column_values_to_be_in_set = expect_column_values_to_be_in_set
DataFrame.expect_column_values_to_be_unique = expect_column_values_to_be_unique
