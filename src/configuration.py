import logging

from keboola.component.exceptions import UserException
from pydantic import BaseModel, Field, ValidationError, field_validator


class LoadingOptions(BaseModel):
    date_since: str | None = Field(default=None, description="Start date for data extraction (YYYY-MM-DD)")
    date_to: str | None = Field(default=None, description="End date for data extraction (YYYY-MM-DD)")


class Endpoints(BaseModel):
    """Endpoints configuration - boolean flags for each endpoint"""

    products: bool = Field(default=False)
    products_drafts: bool = Field(default=False)
    products_archived: bool = Field(default=False)
    orders: bool = Field(default=False)
    customers: bool = Field(default=False)
    inventory: bool = Field(default=False)
    transactions: bool = Field(default=False)
    payments_transactions: bool = Field(default=False)
    product_metafields: bool = Field(default=False)
    variant_metafields: bool = Field(default=False)
    events: list[dict] = Field(default_factory=list)

    # not sure whether we need these
    inventory_items: bool = Field(default=False)
    locations: bool = Field(default=False)

    # legacy endpoints (downloading items in batches instead of bulk download)
    products_legacy: bool = Field(default=False)
    orders_legacy: bool = Field(default=False)
    customers_legacy: bool = Field(default=False)

    def get_enabled_endpoints(self) -> list[str]:
        """Get list of enabled endpoint names"""
        enabled = []
        for field_name, field_value in self.model_dump().items():
            if field_name == "events":
                if field_value:  # If events array is not empty
                    enabled.append("events")
            elif field_value is True:
                enabled.append(field_name)
        return enabled


class Configuration(BaseModel):
    store_name: str = Field(..., description="Shopify store name (without .myshopify.com)")
    api_version: str = Field(default="2025-10", description="Shopify API version")
    api_token: str = Field(alias="#api_token", description="Shopify Admin API access token")
    loading_options: LoadingOptions = Field(default_factory=LoadingOptions)
    endpoints: Endpoints = Field(default_factory=Endpoints, description="Endpoints configuration")
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

    @property
    def shop_url(self) -> str:
        """Get the full Shopify shop URL"""
        return f"https://{self.store_name}.myshopify.com"

    @property
    def enabled_endpoints(self) -> list[str]:
        """Get list of enabled endpoint names"""
        return self.endpoints.get_enabled_endpoints()
