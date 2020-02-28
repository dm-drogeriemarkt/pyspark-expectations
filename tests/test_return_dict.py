from pyspark_expectations import expectations
import pytest

@pytest.fixture
def summary():
    return {"count": 12345, "unexpected_percent": 0.2}

@pytest.fixture
def success():
    return True

def test_value_error_when_parameter_success_has_false_format(summary):
    with pytest.raises(ValueError):
        expectations._return_dict(1, summary)
    with pytest.raises(ValueError):
        expectations._return_dict(None, summary)
    with pytest.raises(ValueError):
        expectations._return_dict("string", summary)

def test_value_error_when_parameter_summary_has_false_format(success):
    with pytest.raises(ValueError):
        expectations._return_dict(success, True)
    with pytest.raises(ValueError):
        expectations._return_dict(success, [1, 2, 3, 4, 5, 6])
    with pytest.raises(ValueError):
        expectations._return_dict(success, {1, 4, 3})
    with pytest.raises(ValueError):
        expectations._return_dict(success, (1, 2, "wwer"))
    with pytest.raises(ValueError):
        expectations._return_dict(success, None)

def test_correct_output(summary, success):
    output = {"success": success, "summary": summary}

    assert expectations._return_dict(success, summary) == output
    assert not expectations._return_dict(success, summary) == {}