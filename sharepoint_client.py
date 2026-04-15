#!/usr/bin/env python3
"""
SharePoint Graph API client for COS Office Occupancy pipeline.

Provides authenticated access to SharePoint document libraries via Microsoft
Graph API using MSAL client-credentials flow (service principal).

Credentials are resolved in order:
1. Databricks secret scope ``cos-sharepoint`` (keys: client-id, client-secret, tenant-id)
2. Environment variables: SP_CLIENT_ID, SP_CLIENT_SECRET, SP_TENANT_ID
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import msal
import requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SHAREPOINT_HOST = "greystar365.sharepoint.com"
SHAREPOINT_SITE_PATH = "/sites/CorporateOfficeStrategy"
DEFAULT_LIBRARY_NAME = "Documents"

GRAPH_BASE = "https://graph.microsoft.com/v1.0"
SCOPE = ["https://graph.microsoft.com/.default"]

# Databricks secret scope / key names
_SCOPE_NAME = "office-occupancy"
_KEY_CLIENT_ID = "client-id"
_KEY_CLIENT_SECRET = "client-secret"
_KEY_TENANT_ID = "tenant-id"


def _get_dbutils():
    """Return dbutils if running inside Databricks, else None."""
    try:
        from databricks.sdk.runtime import dbutils  # noqa: WPS433
        return dbutils
    except Exception:
        pass
    # Fallback: dbutils is injected as a notebook global by Databricks
    import builtins
    return getattr(builtins, "dbutils", None)


@dataclass
class SharePointConfig:
    """Holds service-principal credentials and site info."""

    client_id: str
    client_secret: str
    tenant_id: str
    site_host: str = SHAREPOINT_HOST
    site_path: str = SHAREPOINT_SITE_PATH
    library_name: str = DEFAULT_LIBRARY_NAME

    @classmethod
    def from_secrets(cls, library_name: str = DEFAULT_LIBRARY_NAME) -> "SharePointConfig":
        """Build config from Databricks secrets with env-var fallback."""
        dbu = _get_dbutils()

        def _secret(key: str, env_var: str) -> str:
            if dbu is not None:
                try:
                    return dbu.secrets.get(scope=_SCOPE_NAME, key=key)
                except Exception:
                    pass
            val = os.environ.get(env_var)
            if not val:
                raise RuntimeError(
                    f"Missing credential: set Databricks secret '{_SCOPE_NAME}/{key}' "
                    f"or env var '{env_var}'"
                )
            return val

        return cls(
            client_id=_secret(_KEY_CLIENT_ID, "SP_CLIENT_ID"),
            client_secret=_secret(_KEY_CLIENT_SECRET, "SP_CLIENT_SECRET"),
            tenant_id=_secret(_KEY_TENANT_ID, "SP_TENANT_ID"),
            library_name=library_name,
        )


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------

class SharePointClient:
    """Thin wrapper around Microsoft Graph for SharePoint file operations."""

    def __init__(self, config: SharePointConfig) -> None:
        self._cfg = config
        self._token: Optional[str] = None
        self._site_id: Optional[str] = None
        self._drive_id: Optional[str] = None

    # -- auth ---------------------------------------------------------------

    def _acquire_token(self) -> str:
        if self._token is not None:
            return self._token

        authority = f"https://login.microsoftonline.com/{self._cfg.tenant_id}"
        app = msal.ConfidentialClientApplication(
            self._cfg.client_id,
            authority=authority,
            client_credential=self._cfg.client_secret,
        )
        result = app.acquire_token_for_client(scopes=SCOPE)
        if "access_token" not in result:
            raise RuntimeError(
                f"MSAL token acquisition failed: {result.get('error_description', result)}"
            )
        self._token = result["access_token"]
        return self._token

    def _headers(self) -> Dict[str, str]:
        return {"Authorization": f"Bearer {self._acquire_token()}"}

    # -- Graph helpers ------------------------------------------------------

    def _get_json(self, url: str, params: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        resp = requests.get(url, headers=self._headers(), params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()

    # -- site / drive discovery ---------------------------------------------

    def _get_site_id(self) -> str:
        if self._site_id is not None:
            return self._site_id
        url = f"{GRAPH_BASE}/sites/{self._cfg.site_host}:{self._cfg.site_path}"
        data = self._get_json(url)
        self._site_id = data["id"]
        return self._site_id

    def _get_drive_id(self) -> str:
        if self._drive_id is not None:
            return self._drive_id

        site_id = self._get_site_id()
        url = f"{GRAPH_BASE}/sites/{site_id}/drives"
        data = self._get_json(url)

        drives = data.get("value", [])
        for d in drives:
            if d.get("name") == self._cfg.library_name:
                self._drive_id = d["id"]
                return self._drive_id

        available = [d.get("name") for d in drives]
        raise RuntimeError(
            f"Document library '{self._cfg.library_name}' not found. "
            f"Available libraries: {available}. "
            f"Override with --library flag or edit DEFAULT_LIBRARY_NAME in sharepoint_client.py."
        )

    # -- file operations ----------------------------------------------------

    def list_folder(self, folder_path: str) -> List[Dict[str, Any]]:
        """List .xlsx files in a SharePoint folder.

        Returns list of dicts with keys: id, name, size.
        """
        drive_id = self._get_drive_id()
        url = f"{GRAPH_BASE}/drives/{drive_id}/root:/{folder_path}:/children"
        data = self._get_json(url)
        return [
            {"id": item["id"], "name": item["name"], "size": item.get("size", 0)}
            for item in data.get("value", [])
            if item.get("name", "").lower().endswith(".xlsx")
        ]

    def download_file(self, item_id: str, dest: Path) -> None:
        """Stream-download a file by its Graph item ID."""
        drive_id = self._get_drive_id()
        url = f"{GRAPH_BASE}/drives/{drive_id}/items/{item_id}/content"
        resp = requests.get(url, headers=self._headers(), stream=True, timeout=120)
        resp.raise_for_status()
        dest.parent.mkdir(parents=True, exist_ok=True)
        with open(dest, "wb") as fh:
            for chunk in resp.iter_content(chunk_size=1024 * 64):
                fh.write(chunk)
