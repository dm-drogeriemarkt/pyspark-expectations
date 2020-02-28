import pytest
from pyspark.sql.functions import lit
from pyspark_expectations import expectations

def test_when_some_values_are_null(df):
    res = df.expect_column_values_to_not_be_null("height")
    assert not res["success"]

def test_when_allowed_33_percent_null_values_but_there_is_more_null_values(df):
    # 33.3..3% - null values and 66.6..67 not null values
    res = df.expect_column_values_to_not_be_null("height", 0.33)
    assert not res["success"]

def test_when_allowed_34_percent_null_values_and_there_is_less_null_values(df):
    # 33.3..3% - null values and 66.6..67 not null values
    res = df.expect_column_values_to_not_be_null("height", 0.34)
    assert res["success"]

def test_when_all_values_not_null(df):
    df_all_not_null = df.withColumn("height", lit("bla"))
    res = df_all_not_null.expect_column_values_to_not_be_null("height")
    assert res["success"]

def test_value_error_when_unexpected_percent_has_false_format(df):
    with pytest.raises(ValueError):
        df.expect_column_values_to_not_be_null("height", unexpected_percent="bla")
    with pytest.raises(ValueError):
        df.expect_column_values_to_not_be_null("height", unexpected_percent=-5)
    with pytest.raises(ValueError):
        df.expect_column_values_to_not_be_null("height", unexpected_percent=5)

def test_when_dataframe_is_empty(df):
    df_empty = df.where("height == 3.3")
    assert df_empty.expect_column_values_to_not_be_null(
        "height", unexpected_percent=0.5
    )["success"]
