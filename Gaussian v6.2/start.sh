#!/bin/bash

# Iniciar el Bot Principal (v62) en segundo plano
echo "🚀 Iniciando Bot Principal (v62)..."
python3 bot_v62.py &

# Iniciar el Bot de Pares (Mean Reversion)
echo "🚀 Iniciando Bot de Pares (Pairs Bot)..."
python3 pairs_bot.py
