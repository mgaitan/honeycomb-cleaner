import json

import pytest
import requests
import responses

from honeycomb_cleaner.client import HoneycombClient


@pytest.fixture
def client():
    """Create a test client instance"""
    return HoneycombClient("test_api_key")


def test_init():
    """Test client initialization"""
    api_key = "test_api_key"
    client = HoneycombClient(api_key)

    assert client.api_key == api_key
    assert client.session is not None
    assert client.session.headers["X-Honeycomb-Team"] == api_key
    assert client.session.headers["Content-Type"] == "application/json"


@responses.activate
def test_get_environment_info_success(client):
    """Test successful environment info retrieval"""
    responses.add(
        responses.GET,
        "https://api.honeycomb.io/1/auth",
        json={
            "environment": {"name": "Production", "slug": "production"},
            "team": {"name": "My Team", "slug": "my-team"},
        },
        status=200,
    )

    result = client.get_environment_info()

    assert result == {
        "environment": {"name": "Production", "slug": "production"},
        "team": {"name": "My Team", "slug": "my-team"},
    }


@responses.activate
def test_get_environment_info_partial_data(client):
    """Test environment info with partial data"""
    responses.add(
        responses.GET,
        "https://api.honeycomb.io/1/auth",
        json={"environment": {"name": "Production"}},
        status=200,
    )

    result = client.get_environment_info()

    assert result == {
        "environment": {"name": "Production"},
        "team": {"name": "Unknown", "slug": "unknown"},
    }


@responses.activate
def test_get_environment_info_network_error(client, capsys):
    """Test environment info with network error"""
    responses.add(
        responses.GET,
        "https://api.honeycomb.io/1/auth",
        body=requests.exceptions.ConnectionError("Network error"),
    )

    result = client.get_environment_info()

    captured = capsys.readouterr()
    assert "Error fetching environment info" in captured.out
    assert result == {
        "environment": {"name": "Unknown", "slug": "unknown"},
        "team": {"name": "Unknown", "slug": "unknown"},
    }


@responses.activate
def test_get_columns_success(client):
    """Test successful column retrieval"""
    dataset_slug = "test-dataset"
    columns_data = [
        {"id": "1", "key_name": "column1", "type": "string"},
        {"id": "2", "key_name": "column2", "type": "integer"},
    ]

    responses.add(
        responses.GET,
        f"https://api.honeycomb.io/1/columns/{dataset_slug}",
        json=columns_data,
        status=200,
    )

    result = client.get_columns(dataset_slug)

    assert result == columns_data


@responses.activate
def test_get_columns_unauthorized(client, capsys):
    """Test column retrieval with unauthorized error"""
    dataset_slug = "test-dataset"

    responses.add(
        responses.GET,
        f"https://api.honeycomb.io/1/columns/{dataset_slug}",
        json={"error": "Unauthorized"},
        status=401,
    )

    result = client.get_columns(dataset_slug)

    captured = capsys.readouterr()
    assert "Unauthorized (401)" in captured.out
    assert "API key may lack 'Manage Queries and Columns' permission" in captured.out
    assert result == []


@responses.activate
def test_get_columns_other_error(client, capsys):
    """Test column retrieval with other error"""
    dataset_slug = "test-dataset"

    responses.add(
        responses.GET,
        f"https://api.honeycomb.io/1/columns/{dataset_slug}",
        json={"error": "Internal server error"},
        status=500,
    )

    result = client.get_columns(dataset_slug)

    captured = capsys.readouterr()
    assert f"Error fetching columns for {dataset_slug}" in captured.out
    assert result == []


@responses.activate
def test_delete_column_success(client):
    """Test successful column deletion"""
    dataset_slug = "test-dataset"
    column_id = "column-123"

    responses.add(
        responses.DELETE,
        f"https://api.honeycomb.io/1/columns/{dataset_slug}/{column_id}",
        status=200,
    )

    result = client.delete_column(dataset_slug, column_id)

    assert result is True


@responses.activate
def test_delete_column_error_with_json(client, capsys):
    """Test column deletion with error response containing JSON"""
    dataset_slug = "test-dataset"
    column_id = "column-123"

    responses.add(
        responses.DELETE,
        f"https://api.honeycomb.io/1/columns/{dataset_slug}/{column_id}",
        json={"error": "Column not found"},
        status=404,
    )

    result = client.delete_column(dataset_slug, column_id)

    captured = capsys.readouterr()
    assert f"FAILED - Error 404 deleting column {column_id}" in captured.out
    assert "Column not found" in captured.out
    assert result is False


@responses.activate
def test_delete_column_error_without_json(client, capsys):
    """Test column deletion with error response without JSON"""
    dataset_slug = "test-dataset"
    column_id = "column-123"

    responses.add(
        responses.DELETE,
        f"https://api.honeycomb.io/1/columns/{dataset_slug}/{column_id}",
        body="Internal server error",
        status=500,
    )

    result = client.delete_column(dataset_slug, column_id)

    captured = capsys.readouterr()
    assert f"FAILED - Error 500 deleting column {column_id}" in captured.out
    assert result is False


@responses.activate
def test_delete_column_network_error(client, capsys):
    """Test column deletion with network error"""
    dataset_slug = "test-dataset"
    column_id = "column-123"

    responses.add(
        responses.DELETE,
        f"https://api.honeycomb.io/1/columns/{dataset_slug}/{column_id}",
        body=requests.exceptions.ConnectionError("Network error"),
    )

    result = client.delete_column(dataset_slug, column_id)

    captured = capsys.readouterr()
    assert f"FAILED - Error deleting column {column_id}" in captured.out
    assert result is False


@responses.activate
def test_get_datasets_success(client):
    """Test successful dataset retrieval"""
    datasets_data = [
        {"name": "dataset1", "slug": "dataset-1"},
        {"name": "dataset2", "slug": "dataset-2"},
    ]

    responses.add(
        responses.GET,
        "https://api.honeycomb.io/1/datasets",
        json=datasets_data,
        status=200,
    )

    result = client.get_datasets()

    assert result == datasets_data


@responses.activate
def test_get_datasets_error_exits(client, capsys):
    """Test dataset retrieval error causes system exit"""
    responses.add(
        responses.GET,
        "https://api.honeycomb.io/1/datasets",
        json={"error": "Unauthorized"},
        status=401,
    )

    with pytest.raises(SystemExit):
        client.get_datasets()

    captured = capsys.readouterr()
    assert "Error fetching datasets" in captured.out


@responses.activate
def test_disable_deletion_protection_success(client):
    """Test successful deletion protection disable"""
    dataset_slug = "test-dataset"

    responses.add(
        responses.PUT, f"https://api.honeycomb.io/1/datasets/{dataset_slug}", status=200
    )

    result = client.disable_deletion_protection(dataset_slug)

    assert result is True
    # Check that the correct payload was sent
    assert len(responses.calls) == 1
    request_body = json.loads(responses.calls[0].request.body)
    assert request_body == {"settings": {"delete_protected": False}}


@responses.activate
def test_disable_deletion_protection_error(client, capsys):
    """Test deletion protection disable with error"""
    dataset_slug = "test-dataset"

    responses.add(
        responses.PUT,
        f"https://api.honeycomb.io/1/datasets/{dataset_slug}",
        json={"error": "Forbidden"},
        status=403,
    )

    result = client.disable_deletion_protection(dataset_slug)

    captured = capsys.readouterr()
    assert f"FAILED - Error 403 disabling protection for {dataset_slug}" in captured.out
    assert result is False


@responses.activate
def test_disable_deletion_protection_network_error(client, capsys):
    """Test deletion protection disable with network error"""
    dataset_slug = "test-dataset"

    responses.add(
        responses.PUT,
        f"https://api.honeycomb.io/1/datasets/{dataset_slug}",
        body=requests.exceptions.ConnectionError("Network error"),
    )

    result = client.disable_deletion_protection(dataset_slug)

    captured = capsys.readouterr()
    assert f"FAILED - Error disabling protection for {dataset_slug}" in captured.out
    assert result is False


@responses.activate
def test_delete_dataset_success(client):
    """Test successful dataset deletion"""
    dataset_slug = "test-dataset"

    responses.add(
        responses.DELETE,
        f"https://api.honeycomb.io/1/datasets/{dataset_slug}",
        status=200,
    )

    result = client.delete_dataset(dataset_slug)

    assert result is True


@responses.activate
def test_delete_dataset_with_protection_retry_success(client, capsys):
    """Test dataset deletion with protection retry that succeeds"""
    dataset_slug = "test-dataset"

    # First call: deletion fails with protection error
    responses.add(
        responses.DELETE,
        f"https://api.honeycomb.io/1/datasets/{dataset_slug}",
        json={"error": "Dataset is delete protected"},
        status=409,
    )

    # Second call: disable protection succeeds
    responses.add(
        responses.PUT, f"https://api.honeycomb.io/1/datasets/{dataset_slug}", status=200
    )

    # Third call: deletion succeeds
    responses.add(
        responses.DELETE,
        f"https://api.honeycomb.io/1/datasets/{dataset_slug}",
        status=200,
    )

    result = client.delete_dataset(dataset_slug, disable_protection=True)

    captured = capsys.readouterr()
    assert "deletion protection detected, disabling..." in captured.out
    assert "retrying delete..." in captured.out
    assert result is True


@responses.activate
def test_delete_dataset_with_protection_retry_disable_fails(client, capsys):
    """Test dataset deletion with protection retry where disable fails"""
    dataset_slug = "test-dataset"

    # First call: deletion fails with protection error
    responses.add(
        responses.DELETE,
        f"https://api.honeycomb.io/1/datasets/{dataset_slug}",
        json={"error": "Dataset is delete protected"},
        status=409,
    )

    # Second call: disable protection fails
    responses.add(
        responses.PUT, f"https://api.honeycomb.io/1/datasets/{dataset_slug}", status=403
    )

    result = client.delete_dataset(dataset_slug, disable_protection=True)

    captured = capsys.readouterr()
    assert "deletion protection detected, disabling..." in captured.out
    assert "FAILED - Error 403 disabling protection" in captured.out
    assert result is False


@responses.activate
def test_delete_dataset_with_protection_retry_second_delete_fails(client, capsys):
    """Test dataset deletion with protection retry where second delete fails"""
    dataset_slug = "test-dataset"

    # First call: deletion fails with protection error
    responses.add(
        responses.DELETE,
        f"https://api.honeycomb.io/1/datasets/{dataset_slug}",
        json={"error": "Dataset is delete protected"},
        status=409,
    )

    # Second call: disable protection succeeds
    responses.add(
        responses.PUT, f"https://api.honeycomb.io/1/datasets/{dataset_slug}", status=200
    )

    # Third call: deletion fails
    responses.add(
        responses.DELETE,
        f"https://api.honeycomb.io/1/datasets/{dataset_slug}",
        status=500,
    )

    result = client.delete_dataset(dataset_slug, disable_protection=True)

    captured = capsys.readouterr()
    assert "deletion protection detected, disabling..." in captured.out
    assert "retrying delete..." in captured.out
    assert "FAILED - Error 500 on retry" in captured.out
    assert result is False


@responses.activate
def test_delete_dataset_error_without_protection(client, capsys):
    """Test dataset deletion with error but no protection retry"""
    dataset_slug = "test-dataset"

    responses.add(
        responses.DELETE,
        f"https://api.honeycomb.io/1/datasets/{dataset_slug}",
        json={"error": "Not found"},
        status=404,
    )

    result = client.delete_dataset(dataset_slug)

    captured = capsys.readouterr()
    assert f"FAILED - Error 404 deleting {dataset_slug}" in captured.out
    assert "Not found" in captured.out
    assert result is False


@responses.activate
def test_delete_dataset_network_error(client, capsys):
    """Test dataset deletion with network error"""
    dataset_slug = "test-dataset"

    responses.add(
        responses.DELETE,
        f"https://api.honeycomb.io/1/datasets/{dataset_slug}",
        body=requests.exceptions.ConnectionError("Network error"),
    )

    result = client.delete_dataset(dataset_slug)

    captured = capsys.readouterr()
    assert f"FAILED - Error deleting {dataset_slug}" in captured.out
    assert result is False


def test_is_deletion_protected_text_check(client, mocker):
    """Test _is_deletion_protected with text check"""
    response = mocker.Mock()
    response.text = "This dataset is delete protected"

    result = client._is_deletion_protected(response)
    assert result is True


def test_is_deletion_protected_json_check(client, mocker):
    """Test _is_deletion_protected with JSON check"""
    response = mocker.Mock()
    response.text = ""
    response.json.return_value = {"error": "Dataset is delete protected"}

    result = client._is_deletion_protected(response)
    assert result is True


def test_is_deletion_protected_no_match(client, mocker):
    """Test _is_deletion_protected with no match"""
    response = mocker.Mock()
    response.text = "Other error"
    response.json.return_value = {"error": "Not found"}

    result = client._is_deletion_protected(response)
    assert result is False


def test_is_deletion_protected_json_error(client, mocker):
    """Test _is_deletion_protected with JSON parsing error"""
    response = mocker.Mock()
    response.text = ""
    response.json.side_effect = ValueError("Invalid JSON")

    result = client._is_deletion_protected(response)
    assert result is False


def test_print_delete_error_with_json(client, mocker, capsys):
    """Test _print_delete_error with JSON error details"""
    response = mocker.Mock()
    response.status_code = 404
    response.json.return_value = {"error": "Dataset not found"}

    client._print_delete_error(response, "test-dataset")

    captured = capsys.readouterr()
    assert "FAILED - Error 404 deleting test-dataset" in captured.out
    assert "Dataset not found" in captured.out


def test_print_delete_error_without_json(client, mocker, capsys):
    """Test _print_delete_error without JSON error details"""
    response = mocker.Mock()
    response.status_code = 500
    response.json.side_effect = ValueError("Invalid JSON")

    client._print_delete_error(response, "test-dataset")

    captured = capsys.readouterr()
    assert "FAILED - Error 500 deleting test-dataset" in captured.out
    # Should not contain error details
    assert "→" not in captured.out
