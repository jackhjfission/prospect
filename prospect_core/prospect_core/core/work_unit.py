"""WorkUnit module for representing and managing work items with dependencies and risk calculations."""

from typing import Self

from pydantic import BaseModel, Field, model_validator


class WorkUnit(BaseModel):
    """
    Represents a unit of work with dependencies, success probability, and value calculations.

    WorkUnit models individual work items that can have dependencies on other work units
    and calculates risk-weighted values based on probability of success. Each work unit
    has both a success value (achieved when successful) and an unconditional value
    (achieved regardless of outcome).

    Attributes:
        id: Unique identifier for the WorkUnit.
        dependent_on_success: List of WorkUnit IDs that must succeed before this unit can be performed.
        dependent_on_unconditionally: List of WorkUnit IDs that must be performed before this unit,
            regardless of their success status.
        probability_of_success: Probability of success as a percentage (0-100).
        success_value: Value achieved if the work is successful.
        unconditional_value: Value achieved regardless of the outcome.

    Raises:
        ValueError: If success_value is less than unconditional_value or if probability_of_success
            is outside the valid range [0, 100].
    """

    id: int = Field(description="Unique id for WorkUnit.")
    dependent_on_success: list[int] = Field(
        description="Other WorkUnits which this is dependent on. Can only perform this unit if dependencies are successful."
    )
    dependent_on_unconditionally: list[int] = Field(
        description="Other WorkUnits which this is dependent on. Can only perform this unit regardless of the success status of the dependency."
    )
    probability_of_success: int = Field(
        ge=0, le=100, description="Probabity of success as percentage."
    )
    success_value: int = Field(description="Value of work if successful.")
    unconditional_value: int = Field(description="Value of work regardless of outcome.")

    @model_validator(mode="after")
    def _success_value_ge_unconditional_value(self) -> Self:
        """
        Validates that success_value is greater than or equal to unconditional_value.
        """
        if self.success_value < self.unconditional_value:
            raise ValueError("success_value must be greater than unconditional_value.")
        return self

    @property
    def risk_weighted_value(self) -> float:
        """
        Calculates the risk-weighted value of the work unit.

        The risk-weighted value is calculated as:
            unconditional_value + (success_value - unconditional_value) * (probability_of_success / 100)

        This represents the expected value of the work unit, taking into account both
        the guaranteed unconditional value and the additional value that depends on success,
        weighted by the probability of success.

        Returns:
            float: The risk-weighted value of the work unit.
        """
        return self.unconditional_value + (
            self.success_value - self.unconditional_value
        ) * (self.probability_of_success / 100)
