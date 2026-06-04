from pydantic import BaseModel, field_validator


class Price(BaseModel):
    amount: float

    @field_validator("amount")
    @classmethod
    def validate_amount(cls, v: float) -> float:
        if v <= 0:
            raise ValueError(f"Price must be positive, got {v}")
        if v > 500_000:
            raise ValueError(f"Price unrealistically high: {v}")
        return round(v, 2)

    def __str__(self) -> str:
        return f"USD {self.amount:,.2f}"
