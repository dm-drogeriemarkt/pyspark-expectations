from pyspark.sql.functions import lit
import pytest
from pyspark_expectations import expectations

def test_when_all_values_null(df):
    df_all_null = df.withColumn("height", lit(None))
    res = df_all_null.expect_column_values_to_be_null("height")
    assert res["success"]

def test_when_not_all_values_null(df):
    res = df.expect_column_values_to_be_null("height")
    assert not res["success"]

def test_when_allowed_66_percent_not_null_values_but_there_is_more_not_null_values(df):
    # 33.3..3% - null values and 66.6..67 not null values
    res = df.expect_column_values_to_be_null("height", 0.66)
    assert not res["success"]

def test_when_allowed_67_percent_not_null_values_and_there_is_less_not_null_values(df):
    # 33.3..3% - null values and 66.6..67 not null values
    res = df.expect_column_values_to_be_null("height", 0.67)
    assert res["success"]

def test_value_error_when_column_name_has_false_format(df):
    with pytest.raises(ValueError):
        df.expect_column_values_to_be_null("column_not_existing", 0.67)
    with pytest.raises(ValueError):
        df.expect_column_values_to_be_null(None, 0.67)

def test_value_error_when_unexpected_percent_has_false_format(df):
    with pytest.raises(ValueError):
        df.expect_column_values_to_be_null("height", unexpected_percent="bla")
    with pytest.raises(ValueError):
        df.expect_column_values_to_be_null("height", unexpected_percent=-5)
    with pytest.raises(ValueError):
        df.expect_column_values_to_be_null("height", unexpected_percent=5)

def test_when_dataframe_is_empty(df):
    df_empty = df.where("height == 3.3")
    assert df_empty.expect_column_values_to_be_null("height", unexpected_percent=0.5)[
        "success"
    ]