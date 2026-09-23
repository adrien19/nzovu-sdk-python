from pydantic import BaseModel, Field


class Item(BaseModel):
    name: str = Field(min_length=1)
    quantity: int = Field(gt=0)
    price: float = Field(ge=0, allow_inf_nan=False)


class CartItems(BaseModel):
    items: list[Item] = Field(min_length=1)
