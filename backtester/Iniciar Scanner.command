#!/bin/zsh
cd "/Users/felipe/Desktop/Codigo/Gaussian/backtester"
(sleep 2; open "http://127.0.0.1:5055") &
python3 market_scanner_app.py
