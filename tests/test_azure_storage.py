"""
Unit tests for Azure Storage integration.
"""

from unittest.mock import MagicMock, patch

import pytest

from src.azure_storage import AzureBlobManager


def test_azure_manager_unconfigured():
    manager = AzureBlobManager(connection_string=None, account_name=None, use_managed_identity=False)
    assert manager.is_configured is False

    with pytest.raises(ValueError, match="Azure Storage is not configured"):
        manager._get_client()


def test_azure_manager_upload(tmp_path):
    test_file = tmp_path / "test_raw.csv"
    test_file.write_text("tx_id,amount\n1,100\n", encoding="utf-8")

    manager = AzureBlobManager(
        connection_string="DefaultEndpointsProtocol=https;AccountName=test;AccountKey=dummy;EndpointSuffix=core.windows.net",
        container_name="test-container",
    )

    mock_blob_client = MagicMock()
    mock_blob_client.url = "https://test.blob.core.windows.net/test-container/test_raw.csv"

    mock_container_client = MagicMock()
    mock_container_client.exists.return_value = True
    mock_container_client.get_blob_client.return_value = mock_blob_client

    mock_service_client = MagicMock()
    mock_service_client.get_container_client.return_value = mock_container_client

    with patch("azure.storage.blob.BlobServiceClient.from_connection_string", return_value=mock_service_client):
        url = manager.upload_file(test_file)
        assert url == "https://test.blob.core.windows.net/test-container/test_raw.csv"
        mock_blob_client.upload_blob.assert_called_once()
