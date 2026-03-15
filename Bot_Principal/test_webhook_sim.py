import requests
import json

# Data based on the user's screenshot for LIGHTUSDT
data = {
    "action": "open",
    "trade_id": 111,
    "side": "buy",
    "symbol": "LIGHTUSDT.P",
    "price": 0.1815,
    "sl": 0.1766
}

url = "http://localhost:5002/webhook"

try:
    print(f"Enviando simulación de orden para {data['symbol']}...")
    response = requests.post(url, json=data)
    print(f"Status Code: {response.status_code}")
    print(f"Response JSON: {response.json()}")
except Exception as e:
    print(f"Error conectando al bot local: {e}")
    print("Asegúrate de que 'python3 bot_v62.py' esté corriendo en otra terminal.")
