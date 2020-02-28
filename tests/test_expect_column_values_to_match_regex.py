import pytest
from pyspark_expectations import expectations

def test_column_values_match_regex(df):
    assert df.expect_column_values_to_match_regex("name", "Tanja|Patrik|Tobi")[
        "success"
    ]

def test_not_all_column_values_match_regex(df):
    assert not df.expect_column_values_to_match_regex("name", "Tanja|Patrik|Marco")[
        "success"
    ]

def test_column_values_match_regex_list(df):
    assert (
        df.expect_column_values_to_match_regex("name", ["Tanja", "Patrik", "Tobi"])
    )["success"]

def test_any_column_values_not_match_regex_list(df):
    assert not (
        df.expect_column_values_to_match_regex("name", ["Tanja", "Patrik", "Marco"])
    )["success"]

def test_when_allowed_33_percent_not_matching_regex_values_and_there_is_more_such_values(df):
    assert not df.expect_column_values_to_match_regex("name", ["Tanja", "Patrik"], 0.33)[
        "success"
    ]

def test_when_allowed_334_percent_not_matching_regex_values_and_there_is_less_such_values(df):
    assert df.expect_column_values_to_match_regex("name", ["Tanja", "Patrik"], 0.34)[
        "success"
    ]

def test_value_error_when_column_name_has_false_format(df):
    with pytest.raises(ValueError):
        df.expect_column_values_to_match_regex("column_not_existing", [], 0.67)
    with pytest.raises(ValueError):
        df.expect_column_values_to_match_regex(None, [], 0.67)

def test_value_error_when_unexpected_percent_has_false_format(df):
    with pytest.raises(ValueError):
        df.expect_column_values_to_match_regex(
            "height", ["Tanja", "Patrik"], unexpected_percent="bla"
        )
    with pytest.raises(ValueError):
        df.expect_column_values_to_match_regex(
            "height", ["Tanja", "Patrik"], unexpected_percent=-5
        )
    with pytest.raises(ValueError):
        df.expect_column_values_to_match_regex("height", [], unexpected_percent=5)

def test_value_error_when_regex_list_has_false_format(df):
    with pytest.raises(ValueError):
        df.expect_column_values_to_match_regex("height", 4556, unexpected_percent=0.5)
    with pytest.raises(ValueError):
        df.expect_column_values_to_match_regex("height", None, unexpected_percent=0.5)
    with pytest.raises(ValueError):
        df.expect_column_values_to_match_regex("height", [], unexpected_percent=0.5)
    with pytest.raises(ValueError):
        df.expect_column_values_to_match_regex("height", [1])

def test_when_dataframe_is_empty(df):
    assert df.where("height == 5").expect_column_values_to_match_regex(
        "height", ["Tanja", "Patrik"], unexpected_percent=0.5
    )["success"]
    assert df.where("height == 5").expect_column_values_to_match_regex(
        "height", ["Tanja", "Patrik"]
    )["success"]

