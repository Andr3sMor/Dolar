import unittest
from unittest.mock import patch, MagicMock
import json
import requests  # Asegúrate de importar requests
from datetime import datetime
from dolar_handler import guardar_cotizacion


class TestGuardarCotizacion(unittest.TestCase):

    @patch('dolar_handler.requests.get')
    @patch('dolar_handler.boto3.client')
    @patch('dolar_handler.datetime')  # Mock de datetime
    def test_guardar_cotizacion_success(self, mock_datetime, mock_boto_client, mock_requests_get):
        """
        Test para verificar que la función guardar_cotizacion funciona correctamente
        cuando la solicitud HTTP y la subida a S3 son exitosas.
        """
        # Configurar el mock de datetime para devolver una fecha fija
        mock_datetime.now.return_value = datetime(2025, 9, 10, 12, 0, 0)  # Fecha fija

        # Configurar el mock de requests.get
        mock_respuesta = MagicMock()
        mock_respuesta.raise_for_status = MagicMock()  # No lanza excepciones
        mock_respuesta.json.return_value = {"data": "mocked_data"}  # Datos simulados
        mock_requests_get.return_value = mock_respuesta

        # Configurar el mock de boto3.client
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3

        # Ejecutar la función guardar_cotizacion
        resultado = guardar_cotizacion()

        # Verificar que la solicitud HTTP fue realizada correctamente
        mock_requests_get.assert_called_once_with(
            "https://totoro.banrep.gov.co/estadisticas-economicas/rest/consultaDatosService/consultaMercadoCambiario"
        )

        # Verificar que el archivo fue subido a S3
        mock_s3.put_object.assert_called_once()

        # Verificar el contenido de la llamada a S3
        args, kwargs = mock_s3.put_object.call_args
        self.assertIn("Body", kwargs)  # Asegura que se pasó un cuerpo (Body)
        self.assertEqual(kwargs["ContentType"], "application/json")  # Asegura que el ContentType sea correcto

        # Verificar que la función devuelve el resultado esperado
        self.assertEqual(resultado, {"status": "ok", "file": "dolar/dolar-2025-09-10_12-00-00.json"})

    @patch('dolar_handler.requests.get')
    @patch('dolar_handler.boto3.client')
    def test_guardar_cotizacion_error(self, mock_boto_client, mock_requests_get):
        """
        Test para verificar que la función guardar_cotizacion maneja los errores correctamente
        cuando la solicitud HTTP falla.
        """
        # Configurar el mock de requests.get para simular una excepción
        mock_requests_get.side_effect = requests.exceptions.RequestException("Error de conexión")

        # Ejecutar la función guardar_cotizacion
        resultado = guardar_cotizacion()

        # Verificar que la función maneja el error correctamente
        self.assertEqual(resultado, {"status": "error", "message": "Error de conexión"})

    @patch('dolar_handler.requests.get')
    @patch('dolar_handler.boto3.client')
    def test_guardar_cotizacion_s3_error(self, mock_boto_client, mock_requests_get):
        """
        Test para verificar que la función guardar_cotizacion maneja los errores correctamente
        cuando la subida a S3 falla.
        """
        # Configurar el mock de requests.get para que funcione normalmente
        mock_respuesta = MagicMock()
        mock_respuesta.raise_for_status = MagicMock()  # No lanza excepciones
        mock_respuesta.json.return_value = {"data": "mocked_data"}  # Datos simulados
        mock_requests_get.return_value = mock_respuesta

        # Configurar el mock de boto3.client para que lance una excepción al intentar subir a S3
        mock_s3 = MagicMock()
        mock_s3.put_object.side_effect = Exception("Error subiendo a S3")
        mock_boto_client.return_value = mock_s3

        # Ejecutar la función guardar_cotizacion
        resultado = guardar_cotizacion()

        # Verificar que la función maneja el error de S3 correctamente
        self.assertEqual(resultado, {"status": "error", "message": "Error subiendo a S3"})


if __name__ == '__main__':
    unittest.main()
