import logging

from keboola.component.exceptions import UserException
from pydantic import BaseModel, Field, ValidationError, field_validator


class LoadingOptions(BaseModel):
    date_since: str | None = Field(default=None, description="Start date for data extraction (YYYY-MM-DD)")
    date_to: str | None = Field(default=None, description="End date for data extraction (YYYY-MM-DD)")


class Configuration(BaseModel):
    store_name: str = Field(..., description="Shopify store name (without .myshopify.com)")
    api_version: str = Field(default="2025-10", description="Shopify API version")
    api_token: str = Field(alias="#api_token", description="Shopify Admin API access token")
    loading_options: LoadingOptions = Field(default_factory=LoadingOptions)
    endpoints: list[str] = Field(default=["orders", "products"], description="List of endpoints to extract data from")
    batch_size: int = Field(default=50, ge=1, le=250, description="Number of records per batch")
    debug: bool = Field(default=False, description="Enable debug mode")

    def __init__(self, **data):
        try:
            super().__init__(**data)
        except ValidationError as e:
            error_messages = [f"{err['loc'][0]}: {err['msg']}" for err in e.errors()]
            raise UserException(f"Validation Error: {', '.join(error_messages)}")

        if self.debug:
            logging.debug("Component will run in Debug mode")

    @field_validator("api_token")
    def validate_api_token(cls, v):
        if not v or len(v.strip()) == 0:
            raise UserException("API token cannot be empty")
        return v.strip()

    @field_validator("store_name")
    def validate_store_name(cls, v):
        if not v or len(v.strip()) == 0:
            raise UserException("Store name cannot be empty")
        # Remove .myshopify.com if present
        store_name = v.strip().lower()
        if store_name.endswith(".myshopify.com"):
            store_name = store_name[:-14]
        return store_name

    @field_validator("endpoints")
    def validate_endpoints(cls, v):
        valid_endpoints = [
            "orders",
            "orders_legacy",
            "products",
            "products_legacy",
            "customers",
            "customers_legacy",
            "inventory_items",
            "locations",
            "products_drafts",
            "product_metafields",
            "variant_metafields",
            "inventory",
            "products_archived",
            "transactions",
            "payments_transactions",
            "events",
        ]
        for endpoint in v:
            if endpoint not in valid_endpoints:
                raise UserException(f"Invalid endpoint: {endpoint}. Valid endpoints are: {', '.join(valid_endpoints)}")
        return v

    @property
    def shop_url(self) -> str:
        """Get the full Shopify shop URL"""
        return f"https://{self.store_name}.myshopify.com"
