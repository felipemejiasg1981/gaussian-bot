import time
import os
import sys
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options

# ══════════════════════════════════════════════════════════════╗
# ║  AUTOMATIZACIÓN NINJA (Chrome Remote) — Gaussian v6.2      ║
# ╚══════════════════════════════════════════════════════════════╝

SYMBOLS_FILE = "symbols_v62.txt"

def setup_driver():
    print("🔌 Intentando conectar con tu instancia real de Chrome (Puerto 9222)...")
    chrome_options = Options()
    # Conectar al navegador que ya tienes abierto en modo debug
    chrome_options.add_experimental_option("debuggerAddress", "127.0.0.1:9222")
    
    try:
        driver = webdriver.Chrome(options=chrome_options)
        return driver
    except Exception as e:
        print("\n❌ Error: No se pudo conectar con Chrome.")
        print("👉 Asegúrate de haber cerrado Chrome y vuelto a abrirlo con el comando especial de Depuración.")
        print("👉 Comando: /Applications/Google\\ Chrome.app/Contents/MacOS/Google\\ Chrome --remote-debugging-port=9222")
        sys.exit(1)

def create_alert(driver, symbol):
    print(f"\n📈 Siguiente moneda: {symbol}")
    
    # 1. Ir al gráfico (en la pestaña activa)
    url = f"https://es.tradingview.com/chart/?symbol=BITGET:{symbol}"
    driver.get(url)
    
    print("   ⏳ Cargando gráfico...")
    time.sleep(6) 
    
    # 2. Abrir diálogo de alerta (Alt + A)
    print("   🔔 Abriendo panel de alerta...")
    try:
        body = driver.find_element(By.TAG_NAME, "body")
        body.send_keys(Keys.ALT, 'a')
        
        print(f"   👉 Ahora ve a Chrome, configura la alerta para {symbol} y dale a 'Crear'.")
        input("   ⌨️  PRESIONA ENTER AQUÍ para pasar a la siguiente moneda...")
    except Exception as e:
        print(f"   ❌ Error en {symbol}: {e}")

def main():
    if not os.path.exists(SYMBOLS_FILE):
        print(f"❌ Error: No se encontró el archivo {SYMBOLS_FILE}")
        return

    with open(SYMBOLS_FILE, 'r') as f:
        symbols = [line.strip() for line in f if line.strip()]

    print(f"📋 Cargadas {len(symbols)} monedas.")
    
    driver = setup_driver()
    
    print("\n✅ Conectado exitosamente a tu Chrome real.")
    print("Asegúrate de estar logueado en TradingView en la ventana abierta.")
    
    input("👉 PRESIONA ENTER AQUÍ para empezar el proceso automático...")

    for symbol in symbols:
        create_alert(driver, symbol)

    print("\n✅ Proceso finalizado.")

if __name__ == "__main__":
    main()
