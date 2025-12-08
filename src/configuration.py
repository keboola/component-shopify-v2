import logging

from keboola.component.exceptions import UserException
from pydantic import BaseModel, Field, ValidationError, field_validator

PRODUCTS_ENDPOINTS = {"products", "products_drafts", "products_archived", "products_unlisted"}
EXCLUDE_FROM_ENDPOINTS = {"product_metafields", "variant_metafields", "order_transactions"}


class CustomQuery(BaseModel):
    """Custom GraphQL bulk operation configuration"""

    name: str = Field(..., description="Query name (used for output table name)")
    query: str = Field(..., description="GraphQL bulk operation mutation string")

    @field_validator("name")
    def validate_name(cls, v):
        if not v or len(v.strip()) == 0:
            raise UserException("Custom query name cannot be empty")
        sanitized = v.strip().lower().replace(" ", "_").replace("-", "_")
        return sanitized

    @field_validator("query")
    def validate_query(cls, v):
        if not v or len(v.strip()) == 0:
            raise UserException("Custom bulk operation query cannot be empty")
        return v.strip()


class LoadingOptions(BaseModel):
    date_since: str | None = Field(default=None, description="Start date for data extraction (YYYY-MM-DD)")
    date_to: str | None = Field(default=None, description="End date for data extraction (YYYY-MM-DD)")
    fetch_parameter: str = Field(default="updated_at", description="Field to filter by (updated_at or created_at)")
    incremental_output: int = Field(default=1, description="Load type: 0=Full Load, 1=Incremental Update")


class Endpoints(BaseModel):
    """Endpoints configuration - boolean flags for each endpoint"""

    products: bool = False
    products_drafts: bool = False
    products_archived: bool = False
    products_unlisted: bool = False
    product_metafields: bool = False
    variant_metafields: bool = False
    orders: bool = False
    order_transactions: bool = False
    customers: bool = False
    inventory: bool = False

    # not sure whether we need these
    inventory_items: bool = False
    locations: bool = False

    # legacy endpoints (downloading items in batches instead of bulk download)
    products_legacy: bool = False
    orders_legacy: bool = False
    customers_legacy: bool = False

    def get_enabled_endpoints(self) -> list[str]:
        """Get list of enabled endpoint names (excludes metafield toggles as they're not standalone endpoints)"""
        enabled = []
        for field_name, field_value in self.model_dump().items():
            if field_value and field_name not in EXCLUDE_FROM_ENDPOINTS:
                enabled.append(field_name)
        return enabled


class Configuration(BaseModel):
    store_name: str = Field(..., description="Shopify store name (without .myshopify.com)")
    api_version: str = Field(default="2025-10", description="Shopify API version")
    api_token: str = Field(alias="#api_token", description="Shopify Admin API access token")
    endpoints: Endpoints = Field(default_factory=Endpoints, description="Endpoints configuration")
    events: list[dict] = Field(default_factory=list, description="Events configuration")
    custom_queries: list[CustomQuery] = Field(default_factory=list, description="Custom GraphQL bulk operations")
    loading_options: LoadingOptions = Field(default_factory=LoadingOptions)
    debug: bool = Field(default=False, description="Enable debug mode")

    # keeping as a hidden argument untiil we eventually remove the batch GraphQL endpoints support
    batch_size: int = Field(default=50, ge=1, le=250, description="Number of records per batch")

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
