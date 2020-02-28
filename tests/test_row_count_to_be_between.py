import pytest
from pyspark_expectations import expectations

def test_false_upper_limit(df):
    assert not df.expect_table_row_count_to_be_between(10, 20)["success"]

def test_false_lower_limit(df):
    assert not df.expect_table_row_count_to_be_between(40, 50)["success"]

def test_correct_upper_and_lower_limits(df):
    assert df.expect_table_row_count_to_be_between(20, 40)["success"]

def test_value_error_when_no_limits_given(df):
    with pytest.raises(ValueError):
        df.expect_table_row_count_to_be_between()["success"]

def test_value_error_when_lower_limit_greater_as_upper_limit(df):
    with pytest.raises(ValueError):
        df.expect_table_row_count_to_be_between(10, 5)["success"]

def test_value_error_when_lower_limit_has_false_format(df):
    with pytest.raises(ValueError):
        df.expect_table_row_count_to_be_between("jajajaja", 5)["success"]

def test_value_error_when_upper_limit_has_false_format(df):
    with pytest.raises(ValueError):
        df.expect_table_row_count_to_be_between(5, "ajajaja")["success"]