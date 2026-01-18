from datetime import date, datetime
from decimal import Decimal
from typing import Optional

from pydantic import BaseModel, field_validator, model_validator


class AssetCreate(BaseModel):
    asset_tag: str
    type_id: int
    status_id: int
    manufacturer: Optional[str] = None
    model: Optional[str] = None
    serial_number: Optional[str] = None
    description: Optional[str] = None
    purchase_date: Optional[date] = None
    purchase_price: Optional[Decimal] = None
    currency: Optional[str] = None
    warranty_end: Optional[date] = None
    owner_org_unit_id: Optional[int] = None
    notes: Optional[str] = None

    @field_validator("currency")
    @classmethod
    def uppercase_currency(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        return v.upper()


class AssetUpdate(BaseModel):
    asset_tag: Optional[str] = None
    type_id: Optional[int] = None
    status_id: Optional[int] = None
    manufacturer: Optional[str] = None
    model: Optional[str] = None
    serial_number: Optional[str] = None
    description: Optional[str] = None
    purchase_date: Optional[date] = None
    purchase_price: Optional[Decimal] = None
    currency: Optional[str] = None
    warranty_end: Optional[date] = None
    owner_org_unit_id: Optional[int] = None
    notes: Optional[str] = None

    @field_validator("currency")
    @classmethod
    def uppercase_currency(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        return v.upper()

    @model_validator(mode="after")
    def ensure_any_field(self):
        if not any(value is not None for value in self.model_dump().values()):
            raise ValueError("No fields provided for update")
        return self


class AssignmentCreate(BaseModel):
    asset_id: int
    person_id: Optional[int] = None
    location_id: Optional[int] = None
    assigned_from: Optional[datetime] = None
    assigned_to: Optional[datetime] = None
    purpose: Optional[str] = None
    notes: Optional[str] = None

    @model_validator(mode="after")
    def ensure_target(self):
        if self.person_id is None and self.location_id is None:
            raise ValueError("Either person_id or location_id must be provided")
        return self

    @model_validator(mode="after")
    def ensure_range(self):
        if self.assigned_to and self.assigned_from and self.assigned_to <= self.assigned_from:
            raise ValueError("assigned_to must be greater than assigned_from")
        return self


class AssignmentUpdate(BaseModel):
    assigned_to: Optional[datetime] = None
    purpose: Optional[str] = None
    notes: Optional[str] = None

    @model_validator(mode="after")
    def ensure_any(self):
        if not any(value is not None for value in self.model_dump().values()):
            raise ValueError("No fields provided for update")
        return self
