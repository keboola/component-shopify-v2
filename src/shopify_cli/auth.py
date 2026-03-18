"""
Shopify OAuth token management for client credentials and refresh token flows.

Handles token acquisition, caching in state file, and refresh logic for:
- Client credentials grant (Dev Dashboard apps, 24h token expiry)
- Legacy static access tokens (backward compatible)
"""

import json
import logging
import time

import requests
from keboola.component.exceptions import UserException

logger = logging.getLogger(__name__)

KEY_STATE_TOKEN = "#shopify_token"


class ShopifyTokenManager:
    """Manages Shopify access tokens with support for client credentials grant and state caching."""

    TOKEN_ENDPOINT = "https://{store_name}.myshopify.com/admin/oauth/access_token"

    def __init__(
        self,
        store_name: str,
        client_id: str,
        client_secret: str,
    ):
        self.store_name = store_name
        self.client_id = client_id
        self.client_secret = client_secret
        self._access_token: str = ""
        self._expires_at: float = 0.0

    def get_access_token(self, state: dict | None = None) -> str:
        """Get a valid access token, using cached token from state if still valid.

        Args:
            state: Component state dict that may contain cached token data.

        Returns:
            Valid access token string.
        """
        # Try to load cached token from state
        if state:
            cached = state.get(KEY_STATE_TOKEN)
            if cached:
                token_data = self._load_token_from_state(cached)
                if token_data and self._is_token_valid(token_data):
                    logger.info("Using cached access token from state (still valid)")
                    self._access_token = token_data["access_token"]
                    self._expires_at = token_data.get("expires_at", 0.0)
                    return self._access_token

        # No valid cached token — acquire a new one
        logger.info("Acquiring new access token via client credentials grant")
        self._acquire_token()
        return self._access_token

    def get_token_state(self) -> dict:
        """Get token data to persist in state file.

        Returns:
            Dict with token data suitable for state file storage.
        """
        return {
            KEY_STATE_TOKEN: json.dumps({
                "access_token": self._access_token,
                "expires_at": self._expires_at,
            }),
        }

    def _acquire_token(self) -> None:
        """Exchange client credentials for a new access token."""
        url = self.TOKEN_ENDPOINT.format(store_name=self.store_name)

        data = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }

        try:
            response = requests.post(url, data=data, timeout=30)

            if response.status_code == 401:
                raise UserException(
                    "Shopify authentication failed. Please verify your Client ID and Client Secret "
                    "are correct and that the app is installed on the store."
                )

            if response.status_code == 404:
                raise UserException(
                    f"Shopify store '{self.store_name}' not found. "
                    "Please verify the store name is correct."
                )

            response.raise_for_status()
            token_data = response.json()

            self._access_token = token_data["access_token"]
            expires_in = token_data.get("expires_in", 86400)  # Default 24h
            # Subtract 5-minute buffer so we refresh before actual expiry
            self._expires_at = time.time() + expires_in - 300

            logger.info(
                f"Acquired new access token (expires in {expires_in}s, "
                f"scope: {token_data.get('scope', 'unknown')})"
            )

        except requests.exceptions.ConnectionError as e:
            raise UserException(
                f"Could not connect to Shopify store '{self.store_name}'. "
                "Please verify the store name and your network connection."
            ) from e
        except requests.exceptions.RequestException as e:
            if isinstance(e, requests.exceptions.HTTPError) and e.response is not None:
                try:
                    error_body = e.response.json()
                    error_msg = error_body.get("error_description", error_body.get("error", str(e)))
                except Exception:
                    error_msg = e.response.text or str(e)
                raise UserException(f"Failed to acquire Shopify access token: {error_msg}") from e
            raise UserException(f"Failed to acquire Shopify access token: {e}") from e

    @staticmethod
    def _load_token_from_state(cached: str | dict) -> dict | None:
        """Load token data from state, handling both string and dict formats."""
        try:
            if isinstance(cached, str):
                return json.loads(cached)
            elif isinstance(cached, dict):
                return cached
        except (json.JSONDecodeError, TypeError):
            logger.warning("Could not parse cached token from state, will acquire a new one")
        return None

    @staticmethod
    def _is_token_valid(token_data: dict) -> bool:
        """Check if a cached token is still valid (not expired)."""
        access_token = token_data.get("access_token")
        expires_at = token_data.get("expires_at", 0.0)

        if not access_token:
            return False

        # Check expiration with no additional buffer (buffer already applied during acquisition)
        if time.time() >= expires_at:
            logger.info("Cached token has expired")
            return False

        remaining = expires_at - time.time()
        logger.debug(f"Cached token valid for {remaining:.0f} more seconds")
        return True
