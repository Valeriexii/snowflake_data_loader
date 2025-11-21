import unittest
from data_loader import transform_customers, add_metadata, APIError, fetch_all
from unittest.mock import patch, Mock
import requests


class TestTransforms(unittest.TestCase):
    def test_transform_customers(self):
        input_record = [{
            "id": "cust_001",
            "email": "john.doe@example.com",
            "first_name": "John",
            "last_name": "Doe",
            "phone": "+1-555-0123",
            "address": {
                "street": "123 Main St",
                "city": "New York",
                "state": "NY",
                "zip_code": "10001",
                "country": "USA"
            },
            "created_at": "2024-01-15T10:30:00Z",
            "updated_at": "2024-01-15T10:30:00Z"
        }]

        output = transform_customers(input_record)
        self.assertEqual(output[0]["street"], "123 Main St")
        self.assertEqual(output[0]["city"], "New York")


class TestMetadata(unittest.TestCase):
    def test_add_metadata(self):
        records = [{"id": 1}]
        result = add_metadata(records)
        print(result)
        self.assertIn("_loaded_at", result[0])
        self.assertIn("_source", result[0])


class TestFetchAll(unittest.TestCase):
    @patch("data_loader.requests.get")
    def test_fetch_all_network_error(self, mock_get):
        mock_get.side_effect = requests.RequestException("timeout")
        with self.assertRaises(APIError):
            fetch_all("/api/customers")

    @patch("data_loader.requests.get")
    def test_fetch_all_no_data(self, mock_get):
        """fetch_all should return an empty list when API returns no data."""
        payload = {
            "data": [],
            "pagination": {
                "page": 1,
                "per_page": 100,
                "total": 0,
                "total_pages": 1,
            },
        }

        mock_resp = Mock()
        mock_resp.json.return_value = payload
        mock_resp.raise_for_status.return_value = None
        mock_get.return_value = mock_resp

        records = fetch_all("/api/orders")

        self.assertEqual(records, [])
        self.assertEqual(mock_get.call_count, 1)


if __name__ == "__main__":
    unittest.main()
