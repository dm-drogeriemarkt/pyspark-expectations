import pytest
from pyspark_expectations import expectations

def test_values_in_dataframe_sre_not_in_set(df):    
    res_1 = df.expect_column_values_to_be_in_set("name", ["Tobi","Tanja"])
    assert not res_1['success']

def test_all_values_are_in_set(df):
    res_4 = df.expect_column_values_to_be_in_set("name", ["Tobi","Tanja", "Patrik"])
    assert res_4['success']

def test_when_allowed_34_percent_not_in_set_values_and_there_is_33_percent_not_in_set(df):    
    res_2 = df.expect_column_values_to_be_in_set("name", ["Tobi","Tanja"], 0.34)
    assert res_2['success']

def test_when_allowed_32_percent_not_in_set_values_and_there_is_33_percent_not_in_set(df):   
    res_3 = df.expect_column_values_to_be_in_set("name", ["Tobi","Tanja"], 0.32)
    assert not res_3['success']    

def test_when_dataframe_is_empty(df):
    df_empty = df.where("height == 3.3")
    assert df_empty.expect_column_values_to_be_in_set("name", ["Tobi","Tanja", "Patrik"])["success"]

# Check ValueErrors:
def test_value_error_when_column_name_has_false_format(df):
    with pytest.raises(ValueError):
        df.expect_column_values_to_be_in_set("column_not_exist", [])

    with pytest.raises(ValueError):
        df.expect_column_values_to_be_in_set(12345, [])

def test_value_error_when_unexpected_percent_has_false_format(df):
    with pytest.raises(ValueError):
        df.expect_column_values_to_be_in_set("height",["Tobi","Tanja", "Patrik"], 1.1)

    with pytest.raises(ValueError):
        df.expect_column_values_to_be_in_set("height", ["Tobi","Tanja", "Patrik"],"false_unexpected_percent_value")

def test_value_error_when_value_set_has_false_format(df):
    with pytest.raises(ValueError):
        df.expect_column_values_to_be_in_set("height",None)