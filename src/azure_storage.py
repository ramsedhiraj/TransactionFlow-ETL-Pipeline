"""
TransactionFlow Azure Storage & ADLS Gen2 Integration Module
Provides optional cloud storage integration for uploading and downloading raw transaction files
using Azure Blob Storage or ADLS Gen2, supporting both Connection Strings and Azure Managed Identity.
"""

import logging
from pathlib import Path
from typing import List, Optional, Union

from src.config import settings

logger = logging.getLogger("transactionflow.azure")


class AzureBlobManager:
    """
    Manages interactions with Azure Blob Storage / Azure Data Lake Storage Gen2.
    Gracefully handles absence of cloud credentials during local development.
    """

    def __init__(
        self,
        connection_string: Optional[str] = None,
        container_name: Optional[str] = None,
        account_name: Optional[str] = None,
        use_managed_identity: bool = False,
    ):
        self.connection_string = connection_string or settings.AZURE_STORAGE_CONNECTION_STRING
        self.container_name = container_name or settings.AZURE_STORAGE_CONTAINER_NAME
        self.account_name = account_name or settings.AZURE_ADLS_ACCOUNT_NAME
        self.use_managed_identity = use_managed_identity or settings.AZURE_USE_MANAGED_IDENTITY
        self._blob_service_client = None

    @property
    def is_configured(self) -> bool:
        """Return True if sufficient Azure credentials or account details are configured."""
        return bool(self.connection_string or (self.account_name and self.use_managed_identity))

    def _get_client(self):
        """Lazily initialize Azure BlobServiceClient."""
        if self._blob_service_client is not None:
            return self._blob_service_client

        if not self.is_configured:
            raise ValueError(
                "Azure Storage is not configured. Provide AZURE_STORAGE_CONNECTION_STRING "
                "or set AZURE_ADLS_ACCOUNT_NAME with AZURE_USE_MANAGED_IDENTITY=true."
            )

        try:
            from azure.storage.blob import BlobServiceClient
        except ImportError as err:
            raise ImportError("azure-storage-blob is required for Azure integration.") from err

        if self.connection_string:
            logger.info("Initializing Azure BlobServiceClient via connection string.")
            self._blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)
        elif self.account_name and self.use_managed_identity:
            try:
                from azure.identity import DefaultAzureCredential
            except ImportError as err:
                raise ImportError("azure-identity is required for Managed Identity authentication.") from err

            account_url = f"https://{self.account_name}.blob.core.windows.net"
            logger.info("Initializing Azure BlobServiceClient via Managed Identity for '%s'.", account_url)
            credential = DefaultAzureCredential()
            self._blob_service_client = BlobServiceClient(account_url=account_url, credential=credential)

        return self._blob_service_client

    def upload_file(
        self,
        local_path: Union[str, Path],
        remote_blob_name: Optional[str] = None,
        overwrite: bool = True,
    ) -> str:
        """
        Upload a local file to the configured Azure Blob / ADLS Gen2 container.

        Args:
            local_path: Local file path.
            remote_blob_name: Name of the target blob (defaults to file basename).
            overwrite: Whether to overwrite existing blob.

        Returns:
            URL of the uploaded blob.
        """
        path = Path(local_path).resolve()
        if not path.is_file():
            raise FileNotFoundError(f"Local file not found: {path}")

        blob_name = remote_blob_name or path.name
        client = self._get_client()
        container_client = client.get_container_client(self.container_name)

        if not container_client.exists():
            logger.info("Container '%s' does not exist. Creating container...", self.container_name)
            container_client.create_container()

        blob_client = container_client.get_blob_client(blob_name)
        logger.info("Uploading '%s' to Azure Blob 'container=%s/blob=%s'...", path, self.container_name, blob_name)

        with open(path, "rb") as data:
            blob_client.upload_blob(data, overwrite=overwrite)

        blob_url = blob_client.url
        logger.info("Upload complete: %s", blob_url)
        return blob_url

    def download_file(
        self,
        remote_blob_name: str,
        local_destination_path: Union[str, Path],
    ) -> Path:
        """
        Download a blob from Azure Storage to a local file.
        """
        dest = Path(local_destination_path).resolve()
        dest.parent.mkdir(parents=True, exist_ok=True)

        client = self._get_client()
        blob_client = client.get_blob_client(container=self.container_name, blob=remote_blob_name)

        logger.info("Downloading blob '%s' from container '%s' to '%s'...", remote_blob_name, self.container_name, dest)
        with open(dest, "wb") as f:
            download_stream = blob_client.download_blob()
            f.write(download_stream.readall())

        return dest

    def list_blobs(self, prefix: Optional[str] = None) -> List[str]:
        """List blob names in the container with an optional prefix."""
        client = self._get_client()
        container_client = client.get_container_client(self.container_name)
        if not container_client.exists():
            return []

        return [b.name for b in container_client.list_blobs(name_starts_with=prefix)]
