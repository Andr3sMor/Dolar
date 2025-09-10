import unittest
from unittest.mock import patch
from app import ejecutar_lambda  

class TestLambdaFunction(unittest.TestCase):

    @patch('app.guardar_cotizacion')
    def test_ejecutar_lambda(self, mock_guardar_cotizacion):
        """
        Test para verificar que ejecutar_lambda llama a guardar_cotizacion y devuelve el resultado esperado.
        """
        # Configuramos el mock para que devolver un valor simulado
        mock_guardar_cotizacion.return_value = "Cotización guardada con éxito"
        
        # Ejecutamos la función lambda
        event = {}  # Simulamos un evento vacío
        context = {}  # Simulamos un contexto vacío
        resultado = ejecutar_lambda(event, context)

        # Verificamos que la función guardar_cotizacion haya sido llamada
        mock_guardar_cotizacion.assert_called_once()

        # Verificamos que el resultado devuelto por ejecutar_lambda sea el esperado
        self.assertEqual(resultado, "Cotización guardada con éxito")

if __name__ == '__main__':
    unittest.main()
