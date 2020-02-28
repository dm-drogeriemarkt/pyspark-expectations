import pytest
from pyspark_expectations import expectations

def test_when_values_are_not_unique(df):
    res_1 = df.expect_column_values_to_be_unique("height")
    assert not res_1["success"]

def test_when_allowed_89_percent_not_unique_values_but_there_is_more_not_unique_values(df):
    res_2 = df.expect_column_values_to_be_unique("height", 0.899)
    assert not res_2["success"]

def test_when_allowed_90_percent_not_unique_values_but_there_is_90_percent_unique_values(df):
    res_3 = df.expect_column_values_to_be_unique("height", 0.9)
    assert res_3["success"]

def test_when_values_are_unique(df):
    df_2 = df.distinct()
    res_4 = df_2.expect_column_values_to_be_unique("height")
    assert res_4["success"]

# Check ValueErrors:
def test_value_error_when_column_name_has_false_format(df):
    with pytest.raises(ValueError):
        df.expect_column_values_to_be_unique("column_not_exist")
    with pytest.raises(ValueError):
        df.expect_column_values_to_be_unique(12345)

def test_value_error_when_unexpected_percent_has_false_format(df):
    with pytest.raises(ValueError):
        df.expect_column_values_to_be_unique("height", 1.1)
    with pytest.raises(ValueError):
        df.expect_column_values_to_be_unique("height", "false_unexpected_percent_value")


def test_when_dataframe_is_empty(df):
    df_empty = df.where("height = 2")
    assert df_empty.expect_column_values_to_be_unique("height")
    assert df_empty.expect_column_values_to_be_unique("height", 0.6)