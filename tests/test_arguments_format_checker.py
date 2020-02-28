import pytest
from pyspark_expectations.arguments_format_checker import * 

def test_check_var_type(not_raises):

    # Check iterable <type_list> and Vallue Error
    with pytest.raises(ValueError):
        check_var_type("string", "string", {int, type(0.034), type(None)})
    with pytest.raises(ValueError):
        check_var_type("string", "string", [int, type(0.034), type(None)])
    with pytest.raises(ValueError):
        check_var_type("string", "string", (int, bool, type(None)))

    # Check iterable <type_list> and without Value Error
    with not_raises(ValueError):
        check_var_type("string", "string", (int, bool, type(None), str))
    with not_raises(ValueError):
        check_var_type(
            "string", "string", (int, type(0.034), type(None), str)
        )

    # Check not iterable <type_list> and Value Error
    with pytest.raises(ValueError):
        check_var_type("string", "string", type(None))
    with pytest.raises(ValueError):
        check_var_type("string", "string", int)

    # Check not iterable <type_list> and without Value Error
    with not_raises(ValueError):
        check_var_type("string", "string", str)


def test_check_min_max(not_raises):

    # Check false type of min_value
    with pytest.raises(ValueError):
        check_min_max(type, 10)
    with pytest.raises(ValueError):
        check_min_max("string", 10)

    # Check false type of max_value
    with pytest.raises(ValueError):
        check_min_max(10, type)
    with pytest.raises(ValueError):
        check_min_max(10, "string")

    # Check Value Error when min_value and max_value are None
    with pytest.raises(ValueError):
        check_min_max(None, None)

    # Check Value Error when min_value > max_value
    with pytest.raises(ValueError):
        check_min_max(None, None)

    # Check when everything is correct
    with not_raises(ValueError):
        check_min_max(10, None)
    with not_raises(ValueError):
        check_min_max(None, 10)
    with not_raises(ValueError):
        check_min_max(0, 10)


def test_check_unexpected_percent(not_raises):

    # Check false type of unexpected_percent
    with pytest.raises(ValueError):
        check_unexpected_percent(None)
    with pytest.raises(ValueError):
        check_unexpected_percent("string")

    # Check false range of unexpected_percent
    with pytest.raises(ValueError):
        check_unexpected_percent(12)
    with pytest.raises(ValueError):
        check_unexpected_percent(-5)

    # Check when everything is correct
    with not_raises(ValueError):
        check_unexpected_percent(0)
    with not_raises(ValueError):
        check_unexpected_percent(0.5)
    with not_raises(ValueError):
        check_unexpected_percent(1)


def test_check_column_name(not_raises):

    # Check false type of columns list
    with pytest.raises(ValueError):
        check_column_name("column", None)
    with pytest.raises(ValueError):
        check_column_name("column", 1234)

    # Check false type of column_name
    with pytest.raises(ValueError):
        check_column_name(None, ["name", "height"])
    with pytest.raises(ValueError):
        check_column_name(1, ["name", "height"])

    # Check column_name not in columns list
    with pytest.raises(ValueError):
        check_column_name("firstname", ["name", "height"])

    # Check when everything is correct
    with not_raises(ValueError):
        check_column_name("name", ("name", "height", 1))
    with not_raises(ValueError):
        check_column_name("name", {"name", "height"})
    with not_raises(ValueError):
        check_column_name("name", ["name", "height"])


def test_check_allowed_unexpected_percent():

    # Check false type of unexpected_percent raises Value Error
    with pytest.raises(ValueError):
        check_allowed_unexpected_percent("firstname", 0.3)
    with pytest.raises(ValueError):
        check_allowed_unexpected_percent(None, 0.3)
    with pytest.raises(ValueError):
        check_allowed_unexpected_percent([1, 2, 3, 4], 0.3)

    # Check false type of allowed_unexpected_percent raises Value Error
    with pytest.raises(ValueError):
        check_allowed_unexpected_percent(0.3, "firstname")
    with pytest.raises(ValueError):
        check_allowed_unexpected_percent(0.3, None)
    with pytest.raises(ValueError):
        check_allowed_unexpected_percent(0.3, [1, 2, 3, 4])

    # Check Value Error wenn unexpected_percent not between 0 and 1
    with pytest.raises(ValueError):
        check_allowed_unexpected_percent(40, 0.41)

    # Check Value Error wenn allowed_unexpected_percent not between 0 and 1
    with pytest.raises(ValueError):
        check_allowed_unexpected_percent(0.40, 41)

    # Check unexpected_percent > allowed_unexpected_percent : False
    assert not check_allowed_unexpected_percent(0.41, 0.40)

    # Check unexpected_percent <= allowed_unexpected_percent : True
    assert check_allowed_unexpected_percent(0.39, 0.40)

