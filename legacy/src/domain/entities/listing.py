from dataclasses import dataclass
from typing import Optional

from src.domain.value_objects.mileage import Mileage
from src.domain.value_objects.price import Price


@dataclass(frozen=True)
class Listing:
    id: str
    make: str
    model: str
    year: int
    price: Price
    mileage: Mileage
    state: str
    condition: Optional[str] = None

    def is_valid_year(self) -> bool:
        return 1900 <= self.year <= 2025

    def price_per_mile(self) -> Optional[float]:
        if self.mileage.value == 0:
            return None
        return round(self.price.amount / self.mileage.value, 4)

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "make": self.make,
            "model": self.model,
            "year": self.year,
            "price": self.price.amount,
            "mileage": self.mileage.value,
            "state": self.state,
            "condition": self.condition,
        }
