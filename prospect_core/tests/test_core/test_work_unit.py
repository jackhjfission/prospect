from typing import Any

import pytest

from prospect_core.core import WorkUnit


@pytest.mark.parametrize(
    "updates, expected_match",
    [
        pytest.param(
            {
                "success_value": 60,
                "unconditional_value": 61,
            },
            r"success_value must be greater than unconditional_value\.",
            id="success_value_lt_unconditional_value",
        ),
        pytest.param(
            {
                "probability_of_success": -1,
            },
            r"validation error for WorkUnit\nprobability_of_success\n  Input should be greater than or equal to 0",
            id="risk_outside_lower_bound",
        ),
        pytest.param(
            {
                "probability_of_success": 101,
            },
            r"validation error for WorkUnit\nprobability_of_success\n  Input should be less than or equal to 100",
            id="risk_outside_upper_bound",
        ),
    ],
)
def test_WorkUnit_raises_ValueError(
    updates: dict[str, Any],
    expected_match: str,
) -> None:
    """Test that WorkUnit raises ValueError for invalid field values."""

    obj = {
        "id": 5,
        "dependent_on_success": [],
        "dependent_on_unconditionally": [],
        "probability_of_success": 50,
        "success_value": 60,
        "unconditional_value": 61,
    }
    obj.update(updates)

    with pytest.raises(ValueError, match=expected_match):
        _ = WorkUnit.model_validate(obj=obj)


@pytest.mark.parametrize(
    "updates, expected_risk_weighted_value",
    [
        pytest.param(
            {
                "probability_of_success": 20,
                "success_value": 60,
                "unconditional_value": 30,
            },
            36.0,
            id="sample_1",
        ),
        pytest.param(
            {
                "probability_of_success": 50,
                "success_value": 60,
                "unconditional_value": 30,
            },
            45.0,
            id="sample_2",
        ),
        pytest.param(
            {
                "probability_of_success": 90,
                "success_value": 60,
                "unconditional_value": 30,
            },
            57.0,
            id="sample_3",
        ),
    ],
)
def test_WorkUnit_risk_weighted_value(
    updates: dict[str, Any],
    expected_risk_weighted_value: float,
) -> None:
    """Test that risk_weighted_value is calculated correctly for various probabilities and values.

    Note that for constant success_values and unconditional_values the risk_weighted_value increases
    with probability_of_success.
    """

    obj = {
        "id": 5,
        "dependent_on_success": [],
        "dependent_on_unconditionally": [],
        "probability_of_success": 50,
        "success_value": 60,
        "unconditional_value": 61,
    }
    obj.update(updates)
    work_unit = WorkUnit.model_validate(obj=obj)
    assert work_unit.risk_weighted_value == expected_risk_weighted_value
