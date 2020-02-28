from pyspark_expectations import expectations
import pytest

def test_value_error_when_count_parameter_has_false_format():   
    with pytest.raises(ValueError):
        expectations._return_percent("firstname", 3)
    with pytest.raises(ValueError):
        expectations._return_percent(None, 3)

def test_value_error_when_total_count_parameter_has_false_format():   
    with pytest.raises(ValueError):
        expectations._return_percent(3, "firstname")
    with pytest.raises(ValueError):
        expectations._return_percent(3, None)

def test_when_total_count_equal_to_zero(): 
    assert expectations._return_percent(3, 0) == 0

def test_correct_answers_of_calculations():
    assert expectations._return_percent(3, 10) == float(3) / 10
    assert not expectations._return_percent(3, 10) == float(3) / 11
    assert not expectations._return_percent(3, 10) == float(4) / 11