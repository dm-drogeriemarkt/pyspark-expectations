import pytest
from pyspark_expectations import expectations

@pytest.fixture
def column_to_check():
    return "height"

def test_not_all_values_are_between(df, column_to_check):
    res_1 = df.expect_column_values_to_be_between(column_to_check, 1.91)
    assert not res_1['success']

    res_2 = df.expect_column_values_to_be_between(column_to_check, 1.63, 1.92)
    assert not res_2['success']

def test_when_allowed_32_percent_not_between_values_but_there_is_33_percent_not_between_values(df, column_to_check):
    res_3 = df.expect_column_values_to_be_between(column_to_check,1.62, 1.92, 0.32)
    assert not res_3['success']
    
def test_when_allowed_34_percent_not_between_values_but_there_is_33_percent_not_between_values(df, column_to_check):
    res_4 = df.expect_column_values_to_be_between(column_to_check,1.62, 1.92 ,0.34)
    assert res_4['success']
    
def test_all_values_are_in_range(df, column_to_check):
    df_without_nulls = df.filter(df[column_to_check].isNotNull())

    res_5 = df_without_nulls.expect_column_values_to_be_between(column_to_check, 1.61)
    assert res_5['success']

    res_6 = df_without_nulls.expect_column_values_to_be_between(column_to_check, 0, 1.93)
    assert res_6['success']

    res_7 = df_without_nulls.expect_column_values_to_be_between(column_to_check, 1.62, 1.92)
    assert res_7['success']

def test_when_dataframe_is_empty(df, column_to_check):
    df_empty = df.where("height = 22")
    assert df_empty.expect_column_values_to_be_between(column_to_check, 1.1)['success']

def test_when_nulls_are_ignored_and_all_values_are_in_range(df, column_to_check):    
    res_5 = df.expect_column_values_to_be_between(column_to_check, min_value = 1.1, ignore_null = True)
    assert res_5['success']

#check ValueErrors:
def test_false_max_value_min_value(df, column_to_check):
    with pytest.raises(ValueError):
        df.expect_column_values_to_be_between(column_to_check, 2.80, 1.0)
    with pytest.raises(ValueError):
        df.expect_column_values_to_be_between(column_to_check)

def test_value_error_when_unexpected_percent_has_false_format(df, column_to_check):
    with pytest.raises(ValueError):
        df.expect_column_values_to_be_between(column_to_check, 1.8, 2.0, unexpected_percent="bla")
    with pytest.raises(ValueError):
        df.expect_column_values_to_be_between(column_to_check, 1.8, 2.0, unexpected_percent=-5)
    with pytest.raises(ValueError):
        df.expect_column_values_to_be_between("height", 1.8, 2.0, unexpected_percent=5)

def test_value_error_when_column_name_has_false_format(df, column_to_check):
    with pytest.raises(ValueError):
        df.expect_column_values_to_be_between("false_column_name", 1.8, 2.0, 0.5)
    with pytest.raises(ValueError):
        df.expect_column_values_to_be_between(23455, 1.8, 2.0, 0.5)

    
