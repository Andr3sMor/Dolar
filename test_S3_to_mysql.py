import unittest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
import json

from s3_to_mysql import g

class TestLambdaFunction(unittest.TestCase):
    @patch("boto3.client")
    def test_lambda_success(self, mock_boto_client):
        event = {
            "Records": [{
                "s3": {
                    "bucket": {"name": "dolar-raw-awsmm"},
                    "object": {"key": "dolar-2025-09-10_21-59-28.json"}
                }
            }]
        }

        mock_cursor = Mock()
        mock_db_conn = Mock()
        mock_db_conn.cursor.return_value = mock_cursor

        context = Mock()
        context.db_conn = mock_db_conn

        # Mock cuerpo del objeto S3, pero sin sample data concreto (puede ser vacío JSON array)
        body_bytes = b'[]'

        mock_s3_obj = {
            "Body": MagicMock(read=Mock(return_value=body_bytes))
        }

        mock_boto_client.return_value.get_object.return_value = mock_s3_obj

        result = g(event, context)

        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["rows"], 0)

        insert_query = "INSERT INTO dolar (fechahora, valor) VALUES (%s, %s)"
        mock_cursor.executemany.assert_not_called()  # no hay datos para insertar
        mock_db_conn.commit.assert_called_once()
        mock_cursor.close.assert_called_once()
        mock_db_conn.close.assert_not_called()

    @patch("boto3.client")
    def test_lambda_db_connection_failure(self, mock_boto_client):
        event = {
            "Records": [{
                "s3": {
                    "bucket": {"name": "dolar-raw-awsmm"},
                    "object": {"key": "dolar-2025-09-10_21-59-28.json"}
                }
            }]
        }

        context = Mock()
        context.db_conn = None  # fuerza que intente conectar a DB real

        # Mock boto3 s3 get_object para que devuelva un JSON válido
        sample_data = [
            [1694400000000, "123.45"]
        ]
        body_bytes = json.dumps(sample_data).encode("utf-8")
        mock_s3_obj = {
            "Body": MagicMock(read=Mock(return_value=body_bytes))
        }
        mock_boto_client.return_value.get_object.return_value = mock_s3_obj

        # Parchear pymysql.connect para lanzar excepción (simulando fallo conexión)
        with patch("pymysql.connect", side_effect=Exception("DB connection failed")):
            result = g(event, context)

        self.assertEqual(result["status"], "error")
        self.assertIn("DB connection failed", result["message"])

if __name__ == "__main__":
    unittest.main()
