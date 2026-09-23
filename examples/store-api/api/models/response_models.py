from typing import List, Optional

from pydantic import BaseModel


class CartResponse(BaseModel):
    """Response model for cart operations."""

    status: str
    message: str
    cart_id: Optional[str] = None


class ItemResponse(BaseModel):
    """Response model for individual items."""

    name: str
    quantity: int
    price: float
    subtotal: float


class CheckoutResponse(BaseModel):
    """Response model for checkout operations."""

    cart_id: str
    items: List[ItemResponse]
    total: float
    status: str
    message: str
