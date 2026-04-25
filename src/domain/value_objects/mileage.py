from pydantic import BaseModel, field_validator


class Mileage(BaseModel):
    value: int

    @field_validator("value")
    @classmethod
    def validate_value(cls, v: int) -> int:
        if v < 0:
            raise ValueError(f"Mileage cannot be negative: {v}")
        if v > 1_000_000:
            raise ValueError(f"Mileage unrealistically high: {v}")
        return v
