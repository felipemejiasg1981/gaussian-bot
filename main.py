import sys
import os

# Añadir la carpeta v6.2 al path de Python para permitir la importación
# El nombre de la carpeta tiene espacios, así que lo manejamos dinámicamente
current_dir = os.path.dirname(os.path.abspath(__file__))
v62_dir = os.path.join(current_dir, "Gaussian v6.2")

if os.path.exists(v62_dir):
    sys.path.append(v62_dir)
    try:
        from bot_v62 import app
    except ImportError as e:
        print(f"Error importando bot_v62 desde {v62_dir}: {e}")
        sys.exit(1)
else:
    print(f"Error: No se encontró la carpeta {v62_dir}")
    sys.exit(1)

if __name__ == "__main__":
    # Esto es solo para ejecución local directa
    port = int(os.environ.get("PORT", 5002))
    app.run(host="0.0.0.0", port=port)
