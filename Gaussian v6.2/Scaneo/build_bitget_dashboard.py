#!/usr/bin/env python3
import argparse
import csv
import json
import re
from datetime import datetime, UTC
from pathlib import Path


BASE_DIR = Path(__file__).parent


def latest_file(pattern: str) -> Path:
    files = sorted(
        (
            path for path in BASE_DIR.glob(pattern)
            if not path.name.endswith("_errors.json")
        ),
        key=lambda path: (path.stat().st_mtime, path.name),
    )
    if not files:
        raise FileNotFoundError(f"No files matching {pattern}")
    return files[-1]


def load_rows(csv_path: Path) -> list[dict]:
    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    for row in rows:
        for field in [
            "lookback_days",
            "bars",
            "trades",
            "wins",
            "losses",
            "win_rate_pct",
            "profit_factor",
            "gross_profit_pct",
            "gross_loss_pct",
            "avg_trade_pct",
            "avg_win_pct",
            "avg_loss_pct",
            "expectancy_pct",
            "max_drawdown_pct",
            "net_return_pct",
            "quality_score",
            "last_close",
        ]:
            if field not in row:
                continue
            value = row[field]
            try:
                if field in {"lookback_days", "bars", "trades", "wins", "losses"}:
                    row[field] = int(value)
                else:
                    row[field] = float(value)
            except ValueError:
                if str(value).lower() == "inf":
                    row[field] = float("inf")
                else:
                    row[field] = value
    return rows


def load_summary(summary_path: Path) -> dict:
    return json.loads(summary_path.read_text(encoding="utf-8"))


def top_rows(rows: list[dict], key: str, reverse: bool = True, limit: int = 5) -> list[dict]:
    return sorted(rows, key=lambda row: float(row[key]), reverse=reverse)[:limit]


def compact_row(row: dict) -> dict:
    return {
        "symbol": row["symbol"],
        "win_rate_pct": row["win_rate_pct"],
        "profit_factor": row["profit_factor"],
        "max_drawdown_pct": row["max_drawdown_pct"],
        "net_return_pct": row["net_return_pct"],
        "expectancy_pct": row.get("expectancy_pct", 0),
        "quality_score": row["quality_score"],
        "trades": row["trades"],
        "wins": row.get("wins", 0),
        "losses": row.get("losses", 0),
        "trend_state": row["trend_state"],
    }


def make_payload(rows: list[dict], summary: dict, csv_path: Path, summary_path: Path) -> dict:
    scanner = summary.get("scanner", {})
    avg_winrate = round(sum(float(row["win_rate_pct"]) for row in rows) / len(rows), 2) if rows else 0.0
    avg_pf = round(sum(float(row["profit_factor"]) for row in rows) / len(rows), 3) if rows else 0.0
    avg_dd = round(sum(float(row["max_drawdown_pct"]) for row in rows) / len(rows), 2) if rows else 0.0
    avg_trades = round(sum(int(row["trades"]) for row in rows) / len(rows), 1) if rows else 0.0
    return {
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "source_csv": str(csv_path),
        "source_summary": str(summary_path),
        "scanner": scanner,
        "pine_config": scanner.get("pine_effective_config", {}),
        "pine_not_ported_exactly": scanner.get("pine_not_ported_exactly", []),
        "stats": {
            "symbols": len(rows),
            "avg_winrate_pct": avg_winrate,
            "avg_profit_factor": avg_pf,
            "avg_max_drawdown_pct": avg_dd,
            "avg_trades": avg_trades,
            "best_quality": compact_row(top_rows(rows, "quality_score", True, 1)[0]) if rows else None,
        },
        "leaders": {
            "quality": [compact_row(row) for row in top_rows(rows, "quality_score", True, 8)],
            "winrate": [compact_row(row) for row in top_rows(rows, "win_rate_pct", True, 8)],
            "profit_factor": [compact_row(row) for row in top_rows(rows, "profit_factor", True, 8)],
            "drawdown": [compact_row(row) for row in top_rows(rows, "max_drawdown_pct", True, 8)],
        },
        "rows": rows,
    }


def build_html(payload: dict) -> str:
    data_json = json.dumps(payload, ensure_ascii=False).replace("</", "<\\/")
    template = """<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>__TITLE__</title>
  <style>
    :root {
      --bg: #08111f;
      --bg-2: #0d1729;
      --panel: rgba(12, 21, 38, 0.88);
      --panel-2: rgba(17, 29, 49, 0.92);
      --panel-3: rgba(21, 36, 60, 0.96);
      --line: rgba(147, 178, 222, 0.14);
      --text: #eaf2ff;
      --muted: #8ea2c6;
      --accent: #37c5ff;
      --accent-2: #1fe0a6;
      --accent-3: #ffb454;
      --good: #1fe0a6;
      --warn: #ffb454;
      --bad: #ff6d7a;
      --radius-xl: 28px;
      --radius-lg: 22px;
      --radius-md: 16px;
      --shadow: 0 28px 60px rgba(0, 0, 0, 0.35);
      --sans: "SF Pro Display", "Avenir Next", "Segoe UI", sans-serif;
      --mono: "SFMono-Regular", "Menlo", monospace;
    }
    * { box-sizing: border-box; }
    html, body {
      margin: 0;
      min-height: 100%;
      background:
        radial-gradient(circle at top left, rgba(55, 197, 255, 0.12), transparent 25%),
        radial-gradient(circle at top right, rgba(31, 224, 166, 0.08), transparent 28%),
        linear-gradient(180deg, #08111f 0%, #0b1525 55%, #09111d 100%);
      color: var(--text);
      font-family: var(--sans);
    }
    body { padding: 24px; }
    .shell {
      width: min(1500px, 100%);
      margin: 0 auto;
      display: grid;
      gap: 18px;
    }
    .card {
      background: linear-gradient(180deg, var(--panel) 0%, var(--panel-2) 100%);
      border: 1px solid var(--line);
      box-shadow: var(--shadow);
      border-radius: var(--radius-lg);
    }
    .appbar {
      padding: 22px 24px;
      position: relative;
      overflow: hidden;
      background:
        radial-gradient(circle at left center, rgba(55, 197, 255, 0.12), transparent 24%),
        linear-gradient(180deg, rgba(10, 19, 35, 0.96), rgba(12, 22, 40, 0.98));
    }
    .appbar::before {
      content: "";
      position: absolute;
      inset: 0;
      background:
        radial-gradient(circle at 85% 18%, rgba(55, 197, 255, 0.14), transparent 24%),
        radial-gradient(circle at 10% 95%, rgba(31, 224, 166, 0.08), transparent 24%);
      pointer-events: none;
    }
    .appbar-main {
      position: relative;
      z-index: 1;
      display: grid;
      grid-template-columns: 1.2fr 0.8fr;
      gap: 18px;
      align-items: start;
    }
    .eyebrow {
      color: var(--accent);
      font-size: 11px;
      letter-spacing: 0.18em;
      text-transform: uppercase;
      font-family: var(--mono);
    }
    .appbar-title-row {
      display: flex;
      flex-wrap: wrap;
      gap: 12px;
      align-items: center;
      margin: 8px 0 10px;
    }
    h1 {
      margin: 0;
      font-size: clamp(28px, 4vw, 40px);
      line-height: 0.98;
      letter-spacing: -0.05em;
    }
    .appbar-copy {
      margin: 0;
      max-width: 74ch;
      color: #c7d6f2;
      font-size: 14px;
      line-height: 1.6;
    }
    .appbar-actions {
      display: grid;
      gap: 12px;
      justify-items: end;
      align-content: start;
    }
    .appbar-action-row {
      display: flex;
      flex-wrap: wrap;
      gap: 12px;
      justify-content: flex-end;
      align-items: center;
    }
    .scan-btn {
      border: none;
      border-radius: 999px;
      padding: 13px 18px;
      background: linear-gradient(135deg, var(--accent), #1f87ff);
      color: white;
      font-family: var(--mono);
      font-size: 12px;
      letter-spacing: 0.08em;
      text-transform: uppercase;
      cursor: pointer;
      box-shadow: 0 16px 38px rgba(55, 197, 255, 0.25);
    }
    .scan-btn[disabled] {
      opacity: 0.6;
      cursor: wait;
    }
    .scan-status {
      color: var(--muted);
      font-size: 12px;
      font-family: var(--mono);
    }
    .appbar-meta {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 12px;
    }
    .mini-stat {
      padding: 14px 16px;
      border-radius: 18px;
      background: rgba(255, 255, 255, 0.04);
      border: 1px solid rgba(255, 255, 255, 0.06);
    }
    .mini-stat span {
      display: block;
      font-family: var(--mono);
      text-transform: uppercase;
      letter-spacing: 0.12em;
      font-size: 11px;
      color: var(--muted);
    }
    .mini-stat strong {
      display: block;
      margin-top: 6px;
      font-size: 22px;
      letter-spacing: -0.03em;
      font-weight: 700;
    }
    .mini-pill {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      padding: 8px 11px;
      border-radius: 999px;
      background: rgba(255,255,255,0.05);
      border: 1px solid rgba(255,255,255,0.06);
      color: var(--muted);
      font-size: 11px;
      font-family: var(--mono);
      text-transform: uppercase;
      letter-spacing: 0.08em;
    }
    .summary-drawer {
      overflow: hidden;
    }
    .summary-drawer summary {
      list-style: none;
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 16px;
      padding: 16px 18px;
      cursor: pointer;
      color: var(--text);
      font-weight: 600;
    }
    .summary-drawer summary::-webkit-details-marker { display: none; }
    .summary-drawer[open] summary {
      border-bottom: 1px solid rgba(147, 178, 222, 0.10);
    }
    .summary-copy {
      color: var(--muted);
      font-size: 12px;
      font-weight: 400;
      margin-top: 4px;
    }
    .summary-grid-wrap {
      padding: 16px;
    }
    .overview-grid {
      display: grid;
      grid-template-columns: repeat(6, minmax(0, 1fr));
      gap: 12px;
    }
    .overview-card {
      padding: 16px;
      min-height: 116px;
      display: grid;
      align-content: space-between;
      background: linear-gradient(180deg, rgba(17, 29, 49, 0.95), rgba(13, 22, 39, 0.95));
    }
    .overview-label {
      font-size: 11px;
      text-transform: uppercase;
      letter-spacing: 0.14em;
      color: var(--muted);
      font-family: var(--mono);
    }
    .overview-value {
      font-size: clamp(24px, 2.7vw, 36px);
      line-height: 0.96;
      letter-spacing: -0.04em;
      font-weight: 700;
      margin: 8px 0 6px;
    }
    .overview-foot {
      color: var(--muted);
      font-size: 12px;
      line-height: 1.45;
    }
    .tabs {
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      padding: 6px;
      background: rgba(255,255,255,0.03);
      border: 1px solid var(--line);
      border-radius: 999px;
      width: fit-content;
    }
    .tab-btn, .subtab-btn, .tf-btn {
      border: 1px solid transparent;
      background: transparent;
      color: var(--muted);
      cursor: pointer;
      font-family: var(--mono);
      font-size: 12px;
      letter-spacing: 0.06em;
      text-transform: uppercase;
      transition: 160ms ease;
    }
    .tab-btn {
      padding: 10px 16px;
      border-radius: 999px;
    }
    .tab-btn.active, .subtab-btn.active, .tf-btn.active {
      color: white;
      background: linear-gradient(135deg, rgba(55, 197, 255, 0.22), rgba(31, 224, 166, 0.16));
      border-color: rgba(55, 197, 255, 0.22);
      box-shadow: inset 0 0 0 1px rgba(55, 197, 255, 0.12);
    }
    .tab-panel { display: none; }
    .tab-panel.active { display: grid; gap: 18px; }
    .config-layout {
      display: grid;
      grid-template-columns: 1.25fr 0.75fr;
      gap: 18px;
      align-items: start;
    }
    .section-card {
      padding: 22px;
    }
    .section-head {
      display: flex;
      justify-content: space-between;
      gap: 12px;
      align-items: start;
      margin-bottom: 16px;
    }
    .section-head h2, .section-head h3 {
      margin: 0;
      font-size: 22px;
      letter-spacing: -0.03em;
    }
    .section-head p {
      margin: 6px 0 0;
      color: var(--muted);
      font-size: 13px;
      line-height: 1.5;
    }
    .subtle-pill {
      display: inline-flex;
      align-items: center;
      padding: 8px 10px;
      border-radius: 999px;
      background: rgba(255,255,255,0.04);
      border: 1px solid rgba(255,255,255,0.06);
      color: var(--muted);
      font-size: 11px;
      font-family: var(--mono);
      text-transform: uppercase;
      letter-spacing: 0.08em;
      white-space: nowrap;
    }
    .scan-topbar, .filter-grid {
      display: grid;
      grid-template-columns: repeat(5, minmax(0, 1fr));
      gap: 12px;
      align-items: end;
    }
    .field {
      display: grid;
      gap: 8px;
    }
    .field label {
      color: var(--muted);
      font-size: 11px;
      text-transform: uppercase;
      letter-spacing: 0.12em;
      font-family: var(--mono);
    }
    .field input, .field select {
      width: 100%;
      border: 1px solid rgba(147, 178, 222, 0.16);
      background: rgba(255,255,255,0.04);
      color: var(--text);
      border-radius: 14px;
      padding: 12px 14px;
      font-size: 14px;
      outline: none;
      font-family: var(--mono);
    }
    .field input::placeholder {
      color: #6c80a2;
    }
    .field input:focus, .field select:focus {
      border-color: rgba(55, 197, 255, 0.4);
      box-shadow: 0 0 0 3px rgba(55, 197, 255, 0.10);
    }
    .timeframe-row {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      margin-top: 2px;
    }
    .tf-btn {
      padding: 10px 14px;
      border-radius: 14px;
      background: rgba(255,255,255,0.04);
      border-color: rgba(255,255,255,0.04);
    }
    .settings-grid {
      display: grid;
      gap: 12px;
      margin-top: 16px;
    }
    .settings-group {
      border: 1px solid rgba(147, 178, 222, 0.12);
      border-radius: 18px;
      overflow: hidden;
      background: rgba(255,255,255,0.03);
    }
    .settings-group summary {
      list-style: none;
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 12px;
      padding: 16px 18px;
      cursor: pointer;
      font-family: var(--mono);
      text-transform: uppercase;
      letter-spacing: 0.08em;
      font-size: 12px;
      color: var(--accent);
      background: rgba(55, 197, 255, 0.04);
    }
    .settings-group summary::-webkit-details-marker { display: none; }
    .settings-fields {
      padding: 16px 18px 18px;
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 12px;
      align-items: end;
    }
    .toggle-wrap {
      display: flex;
      gap: 10px;
      align-items: center;
      min-height: 48px;
      padding: 0 2px;
    }
    .toggle-wrap input {
      width: 18px;
      height: 18px;
      accent-color: var(--accent);
      margin: 0;
    }
    .toggle-wrap span {
      color: var(--text);
      font-size: 14px;
      line-height: 1.3;
    }
    .side-stack {
      display: grid;
      gap: 18px;
    }
    .snapshot-list, .insight-list, .top50-list {
      display: grid;
      gap: 10px;
    }
    .snapshot-item, .insight-item, .top50-item {
      display: grid;
      grid-template-columns: auto 1fr auto;
      gap: 12px;
      align-items: center;
      padding: 12px 14px;
      border-radius: 16px;
      background: rgba(255,255,255,0.04);
      border: 1px solid rgba(147, 178, 222, 0.10);
    }
    .rank {
      width: 30px;
      height: 30px;
      border-radius: 50%;
      display: grid;
      place-items: center;
      background: rgba(55, 197, 255, 0.16);
      color: white;
      font-family: var(--mono);
      font-size: 11px;
    }
    .item-main, .coin-main {
      display: grid;
      gap: 2px;
    }
    .item-main strong, .coin-main strong {
      font-size: 14px;
      letter-spacing: -0.02em;
    }
    .item-meta, .coin-meta {
      color: var(--muted);
      font-size: 12px;
      line-height: 1.45;
    }
    .item-value, .coin-side {
      text-align: right;
      font-family: var(--mono);
      font-size: 12px;
    }
    .subtabs {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
    }
    .subtab-btn {
      padding: 10px 14px;
      border-radius: 14px;
      background: rgba(255,255,255,0.04);
      border-color: rgba(255,255,255,0.04);
    }
    .top50-item {
      grid-template-columns: auto 1fr;
      align-items: start;
    }
    .coin-side {
      display: grid;
      gap: 8px;
      margin-top: 10px;
      text-align: left;
    }
    .metric-chips {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      margin-top: 10px;
    }
    .metric-chip {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      padding: 8px 10px;
      border-radius: 999px;
      background: rgba(255,255,255,0.05);
      border: 1px solid rgba(255,255,255,0.06);
      font-family: var(--mono);
      font-size: 11px;
      color: #d8e4fb;
    }
    .metric-chip strong {
      color: white;
      font-weight: 700;
    }
    .map-layout {
      display: grid;
      grid-template-columns: 1.2fr 0.8fr;
      gap: 18px;
      align-items: start;
    }
    .plot-card {
      padding: 22px;
      min-height: 640px;
      display: grid;
      grid-template-rows: auto 1fr auto;
      gap: 14px;
    }
    #plotWrap {
      border-radius: 20px;
      border: 1px solid rgba(147, 178, 222, 0.10);
      background:
        radial-gradient(circle at top left, rgba(55, 197, 255, 0.08), transparent 28%),
        linear-gradient(180deg, rgba(13, 23, 41, 0.94), rgba(11, 18, 32, 0.96));
      overflow: hidden;
      min-height: 480px;
    }
    #plot {
      width: 100%;
      min-height: 480px;
      display: block;
    }
    .legend {
      display: flex;
      flex-wrap: wrap;
      gap: 16px;
      color: var(--muted);
      font-size: 12px;
      font-family: var(--mono);
    }
    .legend span::before {
      content: "";
      display: inline-block;
      width: 10px;
      height: 10px;
      border-radius: 50%;
      margin-right: 8px;
      vertical-align: middle;
    }
    .legend .good::before { background: var(--good); }
    .legend .warn::before { background: var(--accent); }
    .legend .bad::before { background: var(--bad); }
    .table-card {
      padding: 0;
      overflow: hidden;
    }
    .table-head {
      padding: 20px 22px 8px;
    }
    .table-wrap {
      overflow: auto;
      max-height: 980px;
      border-top: 1px solid rgba(147, 178, 222, 0.10);
    }
    table {
      width: 100%;
      min-width: 1160px;
      border-collapse: collapse;
      font-family: var(--mono);
      font-size: 12px;
    }
    thead th {
      position: sticky;
      top: 0;
      z-index: 2;
      background: rgba(9, 16, 30, 0.98);
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.08em;
      cursor: pointer;
    }
    th, td {
      padding: 13px 14px;
      border-bottom: 1px solid rgba(147, 178, 222, 0.08);
      white-space: nowrap;
      text-align: left;
    }
    tbody tr:hover {
      background: rgba(55, 197, 255, 0.05);
    }
    .badge {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-width: 74px;
      padding: 6px 10px;
      border-radius: 999px;
      font-size: 11px;
      text-transform: uppercase;
      letter-spacing: 0.08em;
    }
    .badge.long { background: rgba(31, 224, 166, 0.14); color: var(--good); }
    .badge.short { background: rgba(255, 109, 122, 0.14); color: var(--bad); }
    .badge.none { background: rgba(142, 162, 198, 0.14); color: var(--muted); }
    .good { color: var(--good); }
    .warn { color: var(--warn); }
    .bad { color: var(--bad); }
    .empty-state {
      padding: 22px;
      border-radius: 18px;
      border: 1px dashed rgba(147, 178, 222, 0.16);
      color: var(--muted);
      text-align: center;
      font-size: 14px;
      background: rgba(255,255,255,0.03);
    }
    .footer-note {
      color: var(--muted);
      font-size: 13px;
      line-height: 1.6;
      padding: 0 4px;
    }
    @media (max-width: 1220px) {
      body { padding: 16px; }
      .hero-top, .config-layout, .map-layout, .overview-grid, .scan-topbar, .filter-grid, .settings-fields {
        grid-template-columns: 1fr;
      }
      .tabs { width: 100%; }
      .tab-btn { flex: 1 1 auto; }
    }
  </style>
</head>
<body>
  <div class="shell">
    <section class="card hero">
      <div class="hero-top">
        <div>
          <div class="eyebrow">Bitget Futures Strategy Dashboard</div>
          <h1>Scanner profesional de selección de monedas</h1>
          <p class="hero-copy" id="heroCopy"></p>
          <div class="hero-actions">
            <button id="scanNowBtn" class="scan-btn">Scan now</button>
            <div id="scanStatus" class="scan-status">Listo para ejecutar un nuevo scan.</div>
          </div>
        </div>
        <div class="hero-meta">
          <div class="meta-card">
            <div class="meta-label">Actualizado</div>
            <div class="meta-value" id="generatedAt"></div>
          </div>
          <div class="meta-card">
            <div class="meta-label">Perfil actual</div>
            <div class="meta-value" id="profileValue"></div>
          </div>
          <div class="meta-card">
            <div class="meta-label">Timeframe actual</div>
            <div class="meta-value" id="timeframeValue"></div>
          </div>
          <div class="meta-card">
            <div class="meta-label">Monedas rankeadas</div>
            <div class="meta-value" id="rankedValue"></div>
          </div>
        </div>
      </div>
    </section>

    <section class="overview-grid" id="overviewGrid"></section>

    <nav class="tabs" id="mainTabs">
      <button class="tab-btn active" data-tab="config">Configuración</button>
      <button class="tab-btn" data-tab="top50">Top 50</button>
      <button class="tab-btn" data-tab="map">Mapa</button>
      <button class="tab-btn" data-tab="market">Mercado</button>
    </nav>

    <section id="tab-config" class="tab-panel active">
      <div class="config-layout">
        <div class="card section-card">
          <div class="section-head">
            <div>
              <h2>Configuración del scan</h2>
              <p>Define timeframe, perfil y filtros del strategy antes de lanzar un nuevo escaneo.</p>
            </div>
            <div class="subtle-pill">45m se construye desde velas 15m</div>
          </div>
          <div class="scan-topbar">
            <div class="field">
              <label for="scanProfile">Perfil base</label>
              <select id="scanProfile">
                <option value="base">Base TV</option>
                <option value="memecoins">Memecoins</option>
                <option value="majors">Majors</option>
                <option value="manual">Manual</option>
              </select>
            </div>
            <div class="field">
              <label for="scanLookback">Lookback días</label>
              <input id="scanLookback" type="number" min="3" step="1">
            </div>
            <div class="field">
              <label for="scanMinTrades">Mín trades scan</label>
              <input id="scanMinTrades" type="number" min="0" step="1">
            </div>
            <div class="field">
              <label for="scanMaxSymbols">Máx símbolos</label>
              <input id="scanMaxSymbols" type="number" min="0" step="1" placeholder="Todos">
            </div>
            <div class="field">
              <label for="scanMaxWorkers">Workers</label>
              <input id="scanMaxWorkers" type="number" min="1" max="24" step="1">
            </div>
          </div>
          <div style="margin-top: 14px;">
            <div class="field">
              <label>Timeframe</label>
              <div id="timeframeRow" class="timeframe-row"></div>
            </div>
          </div>
          <div id="scanSettingsGrid" class="settings-grid"></div>
        </div>

        <div class="side-stack">
          <div class="card section-card">
            <div class="section-head">
              <div>
                <h3>Filtros de visualización</h3>
                <p>Estos filtros no cambian el scan guardado; solo ordenan y recortan lo que ves en el dashboard.</p>
              </div>
            </div>
            <div class="filter-grid">
              <div class="field">
                <label for="search">Buscar</label>
                <input id="search" type="text" placeholder="BTC, SOL, meme, AI...">
              </div>
              <div class="field">
                <label for="minTrades">Mín trades</label>
                <input id="minTrades" type="number" min="0" step="1">
              </div>
              <div class="field">
                <label for="minWinrate">Mín winrate %</label>
                <input id="minWinrate" type="number" min="0" step="1">
              </div>
              <div class="field">
                <label for="minPf">Mín PF</label>
                <input id="minPf" type="number" min="0" step="0.1">
              </div>
              <div class="field">
                <label for="maxDd">Máx pérdida %</label>
                <input id="maxDd" type="number" min="0" step="0.5">
              </div>
              <div class="field">
                <label for="trendFilter">Tendencia</label>
                <select id="trendFilter">
                  <option value="ALL">Todas</option>
                  <option value="LONG">LONG</option>
                  <option value="SHORT">SHORT</option>
                </select>
              </div>
              <div class="field">
                <label for="signalFilter">Última señal</label>
                <select id="signalFilter">
                  <option value="ALL">Todas</option>
                  <option value="LONG">LONG</option>
                  <option value="SHORT">SHORT</option>
                  <option value="NONE">NONE</option>
                </select>
              </div>
              <div class="field">
                <label for="sortBy">Ordenar por</label>
                <select id="sortBy">
                  <option value="quality_score">Quality</option>
                  <option value="win_rate_pct">Winrate</option>
                  <option value="profit_factor">Profit factor</option>
                  <option value="max_drawdown_pct">Menor drawdown</option>
                  <option value="net_return_pct">Rentabilidad</option>
                  <option value="expectancy_pct">Expectancy</option>
                  <option value="trades">Trades</option>
                </select>
              </div>
              <div class="field">
                <label for="limitRows">Filas tabla</label>
                <select id="limitRows">
                  <option value="50">50</option>
                  <option value="100">100</option>
                  <option value="250">250</option>
                  <option value="1000">Todo</option>
                </select>
              </div>
              <div class="field">
                <label>&nbsp;</label>
                <button id="resetBtn" class="scan-btn" style="background: linear-gradient(135deg, var(--accent-3), #ff8a2a); box-shadow:none;">Reset filtros</button>
              </div>
            </div>
          </div>

          <div class="card section-card">
            <div class="section-head">
              <div>
                <h3>Resumen del filtro actual</h3>
                <p>Snapshot rápido del subconjunto visible.</p>
              </div>
            </div>
            <div id="filteredStats" class="snapshot-list"></div>
          </div>
        </div>
      </div>
    </section>

    <section id="tab-top50" class="tab-panel">
      <div class="card section-card">
        <div class="section-head">
          <div>
            <h2 id="top50Title">Top 50</h2>
            <p id="top50Copy">Las 50 mejores monedas según la métrica elegida y los filtros activos.</p>
          </div>
          <div class="subtabs" id="topMetricTabs"></div>
        </div>
        <div id="top50List" class="top50-list"></div>
      </div>
    </section>

    <section id="tab-map" class="tab-panel">
      <div class="map-layout">
        <div class="card plot-card">
          <div class="section-head" style="margin-bottom:0;">
            <div>
              <h2>Mapa de oportunidades</h2>
              <p>Eje X: drawdown absoluto. Eje Y: winrate. Tamaño: profit factor. Color: quality score.</p>
            </div>
          </div>
          <div id="plotWrap">
            <svg id="plot" viewBox="0 0 980 540" preserveAspectRatio="xMidYMid meet"></svg>
          </div>
          <div class="legend">
            <span class="good">Quality alto</span>
            <span class="warn">Quality medio</span>
            <span class="bad">Quality débil</span>
          </div>
        </div>

        <div class="side-stack">
          <div class="card section-card">
            <div class="section-head">
              <div>
                <h3>Lectura rápida</h3>
                <p>Los mejores extremos del subconjunto filtrado.</p>
              </div>
            </div>
            <div id="mapInsights" class="insight-list"></div>
          </div>
        </div>
      </div>
    </section>

    <section id="tab-market" class="tab-panel">
      <div class="card table-card">
        <div class="table-head">
          <div class="section-head" style="margin-bottom:0;">
            <div>
              <h2 style="margin:0; font-size:24px;">Mercado analizado</h2>
              <p id="tableCaption" style="margin:6px 0 0; color:var(--muted); font-size:13px;">Listado completo del scan filtrado.</p>
            </div>
          </div>
        </div>
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th data-key="symbol">Symbol</th>
                <th data-key="baseCoin">Base</th>
                <th data-key="trades">Trades</th>
                <th data-key="wins">Wins</th>
                <th data-key="losses">Losses</th>
                <th data-key="win_rate_pct">Winrate %</th>
                <th data-key="profit_factor">PF</th>
                <th data-key="expectancy_pct">Expectancy %</th>
                <th data-key="max_drawdown_pct">Max pérdida %</th>
                <th data-key="net_return_pct">Rentabilidad %</th>
                <th data-key="quality_score">Quality</th>
                <th data-key="trend_state">Trend</th>
                <th data-key="last_signal">Signal</th>
                <th data-key="last_close">Último precio</th>
                <th data-key="last_bar_utc">Última barra</th>
              </tr>
            </thead>
            <tbody id="tableBody"></tbody>
          </table>
        </div>
      </div>
    </section>

    <div class="footer-note">
      La pantalla muestra métricas de estrategia por moneda: winrate, profit factor, drawdown, retorno, expectancy, wins y losses. Todo proviene del último scan guardado y del subconjunto que definas con tus filtros.
    </div>
  </div>

  <script>
    const payload = __DATA__;
    const profileLabelToKey = {
      "Base TV": "base",
      "Memecoins": "memecoins",
      "Majors": "majors",
      "Manual": "manual"
    };
    const profileKeyToLabel = {
      base: "Base TV",
      memecoins: "Memecoins",
      majors: "Majors",
      manual: "Manual"
    };
    const TOP_METRICS = {
      quality_score: { label: "Top quality", hint: "Mejor equilibrio entre winrate, PF, drawdown y trades." },
      win_rate_pct: { label: "Top winrate", hint: "Las que más aciertan dentro del filtro actual." },
      profit_factor: { label: "Top PF", hint: "Las que convierten mejor ganadoras contra perdedoras." },
      max_drawdown_pct: { label: "Menor drawdown", hint: "Las más estables en pérdida máxima dentro del filtro actual." }
    };
    const state = {
      rows: payload.rows.slice(),
      filtered: payload.rows.slice(),
      sortBy: "quality_score",
      sortDir: "desc",
      limit: 50,
      activeTab: "config",
      topMetric: "quality_score",
      scanConfig: { ...(payload.pine_config?.flat || {}) },
      activeTimeframe: payload.scanner?.granularity || payload.pine_config?.flat?.granularity || "4H",
      recommendedLookback: null
    };

    const els = {
      heroCopy: document.getElementById("heroCopy"),
      generatedAt: document.getElementById("generatedAt"),
      profileValue: document.getElementById("profileValue"),
      timeframeValue: document.getElementById("timeframeValue"),
      rankedValue: document.getElementById("rankedValue"),
      scanNowBtn: document.getElementById("scanNowBtn"),
      scanStatus: document.getElementById("scanStatus"),
      overviewGrid: document.getElementById("overviewGrid"),
      mainTabs: document.querySelectorAll(".tab-btn"),
      tabPanels: document.querySelectorAll(".tab-panel"),
      scanProfile: document.getElementById("scanProfile"),
      scanLookback: document.getElementById("scanLookback"),
      scanMinTrades: document.getElementById("scanMinTrades"),
      scanMaxSymbols: document.getElementById("scanMaxSymbols"),
      scanMaxWorkers: document.getElementById("scanMaxWorkers"),
      timeframeRow: document.getElementById("timeframeRow"),
      scanSettingsGrid: document.getElementById("scanSettingsGrid"),
      search: document.getElementById("search"),
      minTrades: document.getElementById("minTrades"),
      minWinrate: document.getElementById("minWinrate"),
      minPf: document.getElementById("minPf"),
      maxDd: document.getElementById("maxDd"),
      trendFilter: document.getElementById("trendFilter"),
      signalFilter: document.getElementById("signalFilter"),
      sortBy: document.getElementById("sortBy"),
      limitRows: document.getElementById("limitRows"),
      resetBtn: document.getElementById("resetBtn"),
      filteredStats: document.getElementById("filteredStats"),
      topMetricTabs: document.getElementById("topMetricTabs"),
      top50Title: document.getElementById("top50Title"),
      top50Copy: document.getElementById("top50Copy"),
      top50List: document.getElementById("top50List"),
      plot: document.getElementById("plot"),
      mapInsights: document.getElementById("mapInsights"),
      tableBody: document.getElementById("tableBody"),
      tableCaption: document.getElementById("tableCaption"),
    };

    const fmt = {
      pct: value => `${Number(value).toFixed(2)}%`,
      num: value => Number(value).toFixed(2),
      pf: value => Number.isFinite(Number(value)) ? Number(value).toFixed(3) : "inf",
      price: value => Number(value).toLocaleString("en-US", { maximumFractionDigits: 8 }),
      dt: value => value ? new Date(value).toLocaleString("es-CL", { dateStyle: "medium", timeStyle: "short" }) : "-",
    };
    const TIMEFRAME_OPTIONS = payload.scanner?.supported_timeframes || ["5m", "15m", "30m", "45m", "1H", "4H"];
    const SETTINGS_SCHEMA = [
      {
        title: "Gaussian Kernel",
        open: true,
        fields: [
          { key: "len_cfg", label: "Longitud", type: "number", min: 5, step: 1 },
          { key: "mode_cfg", label: "Mode", type: "select", options: ["AVG", "MEDIAN", "TRIMMED"] },
          { key: "dist_cfg", label: "Dist", type: "number", step: 0.1 },
        ],
      },
      {
        title: "V6 Extreme Engine",
        fields: [
          { key: "use_adaptive_sigma", label: "Sigma dinámico", type: "checkbox" },
          { key: "sigma_base", label: "Sigma base", type: "number", step: 0.5 },
          { key: "sigma_min", label: "Sigma mín", type: "number", step: 0.5 },
          { key: "sigma_max", label: "Sigma máx", type: "number", step: 0.5 },
        ],
      },
      {
        title: "ADX Trend Strength",
        fields: [
          { key: "use_adx_filter", label: "Filtro ADX", type: "checkbox" },
          { key: "adx_len", label: "ADX Length", type: "number", step: 1 },
          { key: "adx_threshold", label: "ADX umbral", type: "number", step: 1 },
          { key: "use_dmi_confirm", label: "Confirmar DI+/DI-", type: "checkbox" },
        ],
      },
      {
        title: "Market Structure",
        fields: [
          { key: "use_smc_filter", label: "Smart Money Structure", type: "checkbox" },
          { key: "structure_lookback", label: "Lookback estructura", type: "number", step: 1 },
          { key: "swing_strength", label: "Swing strength", type: "number", step: 1 },
          { key: "smc_break_atr_mult", label: "Buffer BOS xATR", type: "number", step: 0.05 },
          { key: "smc_fresh_bars", label: "Ventana fresca", type: "number", step: 1 },
        ],
      },
      {
        title: "Volume Delta",
        fields: [
          { key: "use_volume_absorption", label: "Detección absorción", type: "checkbox" },
          { key: "absorption_threshold", label: "Umbral absorción", type: "number", step: 0.1 },
        ],
      },
      {
        title: "Oscillator Confluence",
        fields: [
          { key: "use_multi_osc", label: "Multi-oscillator", type: "checkbox" },
          { key: "use_rsi", label: "Usar RSI", type: "checkbox" },
          { key: "use_stoch", label: "Usar Stochastic", type: "checkbox" },
          { key: "use_cci", label: "Usar CCI", type: "checkbox" },
          { key: "rsi_len", label: "RSI length", type: "number", step: 1 },
          { key: "stoch_len", label: "Stoch length", type: "number", step: 1 },
          { key: "cci_len", label: "CCI length", type: "number", step: 1 },
          { key: "rsi_oversold", label: "RSI oversold", type: "number", step: 1 },
          { key: "rsi_overbought", label: "RSI overbought", type: "number", step: 1 },
        ],
      },
      {
        title: "Kill Zones",
        fields: [
          { key: "use_kill_zones", label: "Kill zones", type: "checkbox" },
          { key: "kz_london_open", label: "London Open", type: "checkbox" },
          { key: "kz_ny_open", label: "NY Open", type: "checkbox" },
          { key: "kz_london_close", label: "London Close", type: "checkbox" },
          { key: "kz_asia_session", label: "Asia Session", type: "checkbox" },
        ],
      },
      {
        title: "Regime Filters",
        fields: [
          { key: "use_htf_alignment", label: "Alineación HTF", type: "checkbox" },
          { key: "require_dual_htf", label: "Requerir 2 HTF", type: "checkbox" },
          { key: "use_trend_slope", label: "Filtro pendiente", type: "checkbox" },
          { key: "min_trend_slope_atr", label: "Pendiente mín ATR", type: "number", step: 0.01 },
          { key: "use_band_width", label: "Filtro expansión", type: "checkbox" },
          { key: "min_band_width_atr", label: "Ancho mín ATR", type: "number", step: 0.05 },
        ],
      },
      {
        title: "Liquidez & Targets",
        fields: [
          { key: "sl_mode", label: "Modo stop loss", type: "select", options: ["Trend Line", "Estructura (Swing)", "Hibrido"] },
          { key: "target_mode", label: "Modo targets", type: "select", options: ["R Multiples", "Liquidez/HTF", "Hibrido"] },
          { key: "use_liquidity_filter", label: "Filtro liquidez", type: "checkbox" },
          { key: "require_sweep_reject", label: "Bloquear sweep+rechazo", type: "checkbox" },
          { key: "sweep_lookback", label: "Sweep lookback", type: "number", step: 1 },
          { key: "pivot_len", label: "Pivot strength", type: "number", step: 1 },
          { key: "piv_hold_bars", label: "Vida pivots", type: "number", step: 1 },
          { key: "prox_atr", label: "Proximidad opuesta ATR", type: "number", step: 0.1 },
          { key: "break_strength_min", label: "Fuerza mín ruptura", type: "number", step: 0.05 },
          { key: "snap_pct", label: "Tolerancia snap %", type: "number", step: 1 },
        ],
      },
      {
        title: "Divergence Engine",
        fields: [
          { key: "use_divergence", label: "Divergencia avanzada", type: "checkbox" },
          { key: "div_lookback", label: "Lookback divergencia", type: "number", step: 1 },
          { key: "div_pivot_strength", label: "Pivot strength div", type: "number", step: 1 },
        ],
      },
      {
        title: "Choppiness Index",
        fields: [
          { key: "use_chop", label: "Choppiness index", type: "checkbox" },
          { key: "chop_len", label: "CI length", type: "number", step: 1 },
          { key: "chop_threshold", label: "CI max", type: "number", step: 1 },
        ],
      },
      {
        title: "Squeeze Momentum",
        fields: [
          { key: "use_squeeze", label: "Squeeze momentum", type: "checkbox" },
          { key: "sqz_bb_len", label: "BB length", type: "number", step: 1 },
          { key: "sqz_bb_mult", label: "BB mult", type: "number", step: 0.1 },
          { key: "sqz_kc_len", label: "KC length", type: "number", step: 1 },
          { key: "sqz_kc_mult", label: "KC mult", type: "number", step: 0.1 },
          { key: "sqz_lookback", label: "Squeeze lookback", type: "number", step: 1 },
        ],
      },
      {
        title: "WAE Explosion",
        fields: [
          { key: "use_wae", label: "Waddah Attar Explosion", type: "checkbox" },
          { key: "wae_sens", label: "Sensibilidad", type: "number", step: 10 },
          { key: "wae_fast", label: "MACD fast", type: "number", step: 1 },
          { key: "wae_slow", label: "MACD slow", type: "number", step: 1 },
          { key: "wae_bb_len", label: "BB length", type: "number", step: 1 },
          { key: "wae_bb_mult", label: "BB mult", type: "number", step: 0.1 },
          { key: "wae_dead_zone", label: "Dead zone ATR", type: "number", step: 0.1 },
        ],
      },
      {
        title: "Fisher Transform",
        fields: [
          { key: "use_fisher", label: "Fisher transform", type: "checkbox" },
          { key: "fisher_len", label: "Fisher length", type: "number", step: 1 },
          { key: "fisher_extreme", label: "Nivel extremo", type: "number", step: 0.1 },
        ],
      },
      {
        title: "IA Frost Engine",
        fields: [
          { key: "use_frost", label: "IA Frost Engine", type: "checkbox" },
          { key: "min_frost_conf", label: "Min Frost", type: "number", step: 1 },
          { key: "frost_mode", label: "Modo", type: "select", options: ["Scalping", "Intraday", "Swing"] },
        ],
      },
      {
        title: "V6.2 Score Engine",
        fields: [
          { key: "min_score_ratio", label: "Min score ratio", type: "number", step: 0.05 },
        ],
      },
      {
        title: "Re-Entry System",
        fields: [
          { key: "use_reentry", label: "Re-entry 50/50", type: "checkbox" },
          { key: "reentry_bars", label: "Max barras espera", type: "number", step: 1 },
        ],
      },
      {
        title: "Targets & Stop Loss",
        fields: [
          { key: "atr_len", label: "ATR", type: "number", step: 1 },
          { key: "max_sl_pct", label: "SL cap %", type: "number", step: 0.1, scale: 100 },
          { key: "sl_buf", label: "SL buf %", type: "number", step: 0.05, scale: 100 },
          { key: "tp1_r", label: "TP1 R", type: "number", step: 0.1 },
          { key: "tp2_r", label: "TP2 R", type: "number", step: 0.1 },
          { key: "tp3_r", label: "TP3 R", type: "number", step: 0.1 },
          { key: "tp4_r", label: "TP4 R", type: "number", step: 0.001 },
          { key: "pct_tp1", label: "% TP1", type: "number", step: 1, scale: 100 },
          { key: "pct_tp2", label: "% TP2", type: "number", step: 1, scale: 100 },
          { key: "pct_tp3", label: "% TP3", type: "number", step: 1, scale: 100 },
          { key: "pct_runner", label: "% Runner", type: "number", step: 1, scale: 100 },
        ],
      },
      {
        title: "Break Even",
        fields: [
          { key: "fee_pct", label: "BE fee %", type: "number", step: 0.01, scale: 100 },
        ],
      },
    ];

    function settingDef(key) {
      for (const section of SETTINGS_SCHEMA) {
        const field = section.fields.find(item => item.key === key);
        if (field) return field;
      }
      return null;
    }

    function displayValue(key, value) {
      const def = settingDef(key);
      if (!def) return value;
      if (def.scale) return Number(value) * def.scale;
      return value;
    }

    function parseSettingValue(key, raw) {
      const def = settingDef(key);
      if (!def) return raw;
      const numeric = Number(raw);
      if (def.type === "number" && def.scale) return numeric / def.scale;
      return def.type === "number" ? numeric : raw;
    }

    function suggestedLookback(tf) {
      return {
        "5m": 14,
        "15m": 30,
        "30m": 45,
        "45m": 60,
        "1H": 120,
        "4H": 365
      }[tf] || 365;
    }

    function avg(rows, key) {
      if (!rows.length) return 0;
      const values = rows.map(row => Number(row[key] || 0)).filter(value => Number.isFinite(value));
      if (!values.length) return 0;
      return values.reduce((sum, value) => sum + value, 0) / values.length;
    }

    function setMainTab(tab) {
      state.activeTab = tab;
      els.mainTabs.forEach(btn => btn.classList.toggle("active", btn.dataset.tab === tab));
      els.tabPanels.forEach(panel => panel.classList.toggle("active", panel.id === `tab-${tab}`));
    }

    function hero() {
      const scanner = payload.scanner || {};
      els.heroCopy.textContent = `Compara futuros perpetuos de Bitget con métricas de strategy por moneda. El dashboard prioriza datos útiles: winrate, profit factor, drawdown, expectancy, retorno y calidad general para ayudarte a escoger las mejores oportunidades.`;
      els.generatedAt.textContent = fmt.dt(payload.generated_at_utc);
      els.profileValue.textContent = scanner.profile || "-";
      els.timeframeValue.textContent = scanner.granularity || "-";
      els.rankedValue.textContent = `${payload.rows.length}`;
      els.scanProfile.value = profileLabelToKey[scanner.profile] || "base";
      els.scanLookback.value = payload.pine_config?.flat?.lookback_days ?? scanner.lookback_days ?? suggestedLookback(state.activeTimeframe);
      els.scanMinTrades.value = payload.pine_config?.flat?.min_trades ?? scanner.min_trades ?? 0;
      els.scanMaxWorkers.value = payload.pine_config?.flat?.max_workers ?? 8;
      els.scanMaxSymbols.value = payload.pine_config?.flat?.max_symbols ?? "";
      els.minTrades.value = scanner.min_trades ?? 0;
      els.minWinrate.value = 0;
      els.minPf.value = 0;
      els.maxDd.value = 100;
      els.limitRows.value = "50";
      state.recommendedLookback = suggestedLookback(state.activeTimeframe);
      renderTimeframeButtons();
      renderScanSettings();
      renderOverview(payload.rows);
      renderTopMetricTabs();
    }

    function renderOverview(rows) {
      const best = rows.slice().sort((a, b) => b.quality_score - a.quality_score)[0];
      const cards = [
        ["Monedas visibles", rows.length, "Resultado después de aplicar tus filtros."],
        ["Winrate medio", fmt.pct(avg(rows, "win_rate_pct")), "Promedio del subconjunto actual."],
        ["PF medio", fmt.pf(avg(rows, "profit_factor")), "Profit factor medio del filtro actual."],
        ["Expectancy", fmt.pct(avg(rows, "expectancy_pct")), "Retorno esperado por trade."],
        ["Drawdown medio", fmt.pct(avg(rows, "max_drawdown_pct")), "Mientras menos negativo, mejor."],
        ["Mejor actual", best ? best.symbol : "-", best ? `Score ${fmt.num(best.quality_score)}` : "Sin datos"],
      ];
      els.overviewGrid.innerHTML = cards.map(([label, value, foot]) => `
        <article class="card overview-card">
          <div>
            <div class="overview-label">${label}</div>
            <div class="overview-value">${value}</div>
          </div>
          <div class="overview-foot">${foot}</div>
        </article>
      `).join("");
    }

    function renderTimeframeButtons() {
      els.timeframeRow.innerHTML = TIMEFRAME_OPTIONS.map(tf => `
        <button type="button" class="tf-btn ${state.activeTimeframe === tf ? "active" : ""}" data-tf="${tf}">${tf}</button>
      `).join("");
      els.timeframeRow.querySelectorAll(".tf-btn").forEach(btn => {
        btn.addEventListener("click", () => {
          const previous = state.recommendedLookback;
          state.activeTimeframe = btn.dataset.tf;
          const nextSuggested = suggestedLookback(state.activeTimeframe);
          const currentLookback = Number(els.scanLookback.value || 0);
          if (!currentLookback || currentLookback === previous) {
            els.scanLookback.value = nextSuggested;
          }
          state.recommendedLookback = nextSuggested;
          renderTimeframeButtons();
        });
      });
    }

    function fieldMarkup(field, value) {
      if (field.type === "checkbox") {
        return `
          <label class="toggle-wrap">
            <input type="checkbox" data-scan-key="${field.key}" ${value ? "checked" : ""}>
            <span>${field.label}</span>
          </label>
        `;
      }
      if (field.type === "select") {
        return `
          <div class="field">
            <label for="scan_${field.key}">${field.label}</label>
            <select id="scan_${field.key}" data-scan-key="${field.key}">
              ${field.options.map(option => `<option value="${option}" ${String(value) === String(option) ? "selected" : ""}>${option}</option>`).join("")}
            </select>
          </div>
        `;
      }
      return `
        <div class="field">
          <label for="scan_${field.key}">${field.label}</label>
          <input id="scan_${field.key}" type="number" data-scan-key="${field.key}" value="${value ?? ""}" step="${field.step ?? "any"}" min="${field.min ?? ""}">
        </div>
      `;
    }

    function renderScanSettings() {
      const cfg = state.scanConfig;
      els.scanSettingsGrid.innerHTML = SETTINGS_SCHEMA.map(section => `
        <details class="settings-group" ${section.open ? "open" : ""}>
          <summary><span>${section.title}</span><span>${section.fields.length} campos</span></summary>
          <div class="settings-fields">
            ${section.fields.map(field => fieldMarkup(field, displayValue(field.key, cfg[field.key]))).join("")}
          </div>
        </details>
      `).join("");
    }

    function collectScanConfig() {
      const flat = { ...state.scanConfig };
      flat.profile = els.scanProfile.value;
      flat.granularity = state.activeTimeframe;
      flat.lookback_days = Number(els.scanLookback.value || suggestedLookback(state.activeTimeframe));
      flat.min_trades = Number(els.scanMinTrades.value || 0);
      flat.max_workers = Number(els.scanMaxWorkers.value || 8);
      flat.top = 20;
      flat.max_symbols = els.scanMaxSymbols.value ? Number(els.scanMaxSymbols.value) : null;
      document.querySelectorAll("[data-scan-key]").forEach(input => {
        const key = input.dataset.scanKey;
        const def = settingDef(key);
        if (!def) return;
        if (def.type === "checkbox") {
          flat[key] = input.checked;
        } else {
          flat[key] = parseSettingValue(key, input.value);
        }
      });
      return flat;
    }

    async function triggerScan() {
      if (window.location.protocol === "file:") {
        els.scanStatus.textContent = "Abre la dashboard desde el servidor local para lanzar scans.";
        return;
      }
      const payloadToSend = collectScanConfig();
      els.scanNowBtn.disabled = true;
      els.scanStatus.textContent = `Escaneando Bitget en ${payloadToSend.granularity} con la configuración activa...`;
      try {
        const response = await fetch("/api/scan", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payloadToSend),
        });
        const result = await response.json();
        if (!response.ok) throw new Error(result.error || "scan failed");
        els.scanStatus.textContent = "Scan terminado. Recargando dashboard...";
        window.location.reload();
      } catch (error) {
        els.scanStatus.textContent = `Error al escanear: ${error.message}`;
      } finally {
        els.scanNowBtn.disabled = false;
      }
    }

    function sortRows(rows) {
      const key = state.sortBy;
      const dir = state.sortDir === "asc" ? 1 : -1;
      return rows.slice().sort((a, b) => {
        const av = a[key];
        const bv = b[key];
        if (typeof av === "number" && typeof bv === "number") return (av - bv) * dir;
        return String(av).localeCompare(String(bv)) * dir;
      });
    }

    function applyFilters() {
      const q = els.search.value.trim().toLowerCase();
      const minTrades = Number(els.minTrades.value || 0);
      const minWinrate = Number(els.minWinrate.value || 0);
      const minPf = Number(els.minPf.value || 0);
      const maxDdAbs = Number(els.maxDd.value || 100);
      const trend = els.trendFilter.value;
      const signal = els.signalFilter.value;
      state.sortBy = els.sortBy.value;
      state.limit = Number(els.limitRows.value);
      state.sortDir = state.sortBy === "symbol" || state.sortBy === "baseCoin" ? "asc" : "desc";

      const filtered = payload.rows.filter(row => {
        const matchesQ = !q || row.symbol.toLowerCase().includes(q) || row.baseCoin.toLowerCase().includes(q);
        const matchesTrades = Number(row.trades) >= minTrades;
        const matchesWinrate = Number(row.win_rate_pct) >= minWinrate;
        const matchesPf = Number(row.profit_factor) >= minPf;
        const matchesDd = Math.abs(Number(row.max_drawdown_pct)) <= maxDdAbs;
        const matchesTrend = trend === "ALL" || row.trend_state === trend;
        const matchesSignal = signal === "ALL" || row.last_signal === signal;
        return matchesQ && matchesTrades && matchesWinrate && matchesPf && matchesDd && matchesTrend && matchesSignal;
      });

      state.filtered = sortRows(filtered);
      renderAll();
    }

    function badge(value) {
      const cls = value === "LONG" ? "long" : value === "SHORT" ? "short" : "none";
      return `<span class="badge ${cls}">${value}</span>`;
    }

    function qualityClass(value) {
      if (value >= 4) return "good";
      if (value >= 1.5) return "warn";
      return "bad";
    }

    function renderFilteredStats(rows) {
      if (!rows.length) {
        els.filteredStats.innerHTML = `<div class="empty-state">No hay monedas que cumplan el filtro actual.</div>`;
        return;
      }
      const best = rows.slice().sort((a, b) => b.quality_score - a.quality_score)[0];
      const bestWin = rows.slice().sort((a, b) => b.win_rate_pct - a.win_rate_pct)[0];
      const bestPf = rows.slice().sort((a, b) => b.profit_factor - a.profit_factor)[0];
      const bestDd = rows.slice().sort((a, b) => b.max_drawdown_pct - a.max_drawdown_pct)[0];
      const items = [
        ["Mejor quality", best ? `${best.symbol}` : "-", best ? `Score ${fmt.num(best.quality_score)}` : "-"],
        ["Mejor winrate", bestWin ? `${bestWin.symbol}` : "-", bestWin ? fmt.pct(bestWin.win_rate_pct) : "-"],
        ["Mejor PF", bestPf ? `${bestPf.symbol}` : "-", bestPf ? `PF ${fmt.pf(bestPf.profit_factor)}` : "-"],
        ["Menor drawdown", bestDd ? `${bestDd.symbol}` : "-", bestDd ? fmt.pct(bestDd.max_drawdown_pct) : "-"],
        ["Trades medios", `${avg(rows, "trades").toFixed(1)}`, "Promedio del filtro"],
        ["Expectancy media", fmt.pct(avg(rows, "expectancy_pct")), "Promedio del filtro"],
      ];
      els.filteredStats.innerHTML = items.map(([title, value, meta], index) => `
        <div class="snapshot-item">
          <div class="rank">${index + 1}</div>
          <div class="item-main">
            <strong>${title}</strong>
            <div class="item-meta">${meta}</div>
          </div>
          <div class="item-value">${value}</div>
        </div>
      `).join("");
    }

    function renderTopMetricTabs() {
      els.topMetricTabs.innerHTML = Object.entries(TOP_METRICS).map(([key, meta]) => `
        <button class="subtab-btn ${state.topMetric === key ? "active" : ""}" data-metric="${key}">${meta.label}</button>
      `).join("");
      els.topMetricTabs.querySelectorAll(".subtab-btn").forEach(btn => {
        btn.addEventListener("click", () => {
          state.topMetric = btn.dataset.metric;
          renderTopMetricTabs();
          renderTop50(state.filtered);
        });
      });
    }

    function topRowsByMetric(rows, metric) {
      const sorted = rows.slice().sort((a, b) => {
        if (metric === "symbol") return String(a.symbol).localeCompare(String(b.symbol));
        return Number(b[metric]) - Number(a[metric]);
      });
      return sorted.slice(0, 50);
    }

    function renderTop50(rows) {
      const meta = TOP_METRICS[state.topMetric];
      els.top50Title.textContent = meta.label;
      els.top50Copy.textContent = meta.hint;
      const topRows = topRowsByMetric(rows, state.topMetric);
      if (!topRows.length) {
        els.top50List.innerHTML = `<div class="empty-state">No hay monedas para mostrar en el Top 50 con el filtro actual.</div>`;
        return;
      }
      els.top50List.innerHTML = topRows.map((row, index) => `
        <div class="top50-item">
          <div class="rank">${index + 1}</div>
          <div class="coin-main">
            <strong>${row.symbol}</strong>
            <div class="coin-meta">${row.baseCoin} · ${row.trades} trades · ${row.wins}W / ${row.losses}L · ${row.trend_state}</div>
            <div class="metric-chips">
              <span class="metric-chip">WR <strong>${fmt.pct(row.win_rate_pct)}</strong></span>
              <span class="metric-chip">PF <strong>${fmt.pf(row.profit_factor)}</strong></span>
              <span class="metric-chip">DD <strong>${fmt.pct(row.max_drawdown_pct)}</strong></span>
              <span class="metric-chip">RET <strong>${fmt.pct(row.net_return_pct)}</strong></span>
              <span class="metric-chip">EXP <strong>${fmt.pct(row.expectancy_pct)}</strong></span>
            </div>
          </div>
        </div>
      `).join("");
    }

    function colorForQuality(value) {
      if (value >= 6) return "rgba(31, 224, 166, 0.92)";
      if (value >= 2) return "rgba(55, 197, 255, 0.84)";
      if (value >= 1) return "rgba(255, 180, 84, 0.80)";
      return "rgba(255, 109, 122, 0.78)";
    }

    function renderMapInsights(rows) {
      if (!rows.length) {
        els.mapInsights.innerHTML = `<div class="empty-state">Sin datos para construir insights del mapa.</div>`;
        return;
      }
      const bestQuality = rows.slice().sort((a, b) => b.quality_score - a.quality_score)[0];
      const bestWin = rows.slice().sort((a, b) => b.win_rate_pct - a.win_rate_pct)[0];
      const bestPf = rows.slice().sort((a, b) => b.profit_factor - a.profit_factor)[0];
      const bestDd = rows.slice().sort((a, b) => b.max_drawdown_pct - a.max_drawdown_pct)[0];
      const items = [
        ["Líder quality", bestQuality.symbol, `Score ${fmt.num(bestQuality.quality_score)}`],
        ["Winrate más alto", bestWin.symbol, fmt.pct(bestWin.win_rate_pct)],
        ["PF más alto", bestPf.symbol, `PF ${fmt.pf(bestPf.profit_factor)}`],
        ["Menor drawdown", bestDd.symbol, fmt.pct(bestDd.max_drawdown_pct)],
      ];
      els.mapInsights.innerHTML = items.map(([title, value, meta], index) => `
        <div class="insight-item">
          <div class="rank">${index + 1}</div>
          <div class="item-main">
            <strong>${title}</strong>
            <div class="item-meta">${meta}</div>
          </div>
          <div class="item-value">${value}</div>
        </div>
      `).join("");
    }

    function renderPlot(rows) {
      const svg = els.plot;
      const width = 980;
      const height = 540;
      const padX = 72;
      const padY = 54;
      const innerW = width - padX * 2;
      const innerH = height - padY * 2;
      svg.innerHTML = "";
      const subset = rows.slice().sort((a, b) => b.quality_score - a.quality_score).slice(0, 140);
      if (!subset.length) {
        return;
      }

      const xVals = subset.map(row => Math.abs(Number(row.max_drawdown_pct)));
      const yVals = subset.map(row => Number(row.win_rate_pct));
      const xMax = Math.max(...xVals, 5);
      const yMin = 0;
      const yMax = Math.max(...yVals, 100);
      const xScale = value => padX + (Math.abs(value) / xMax) * innerW;
      const yScale = value => height - padY - ((value - yMin) / (yMax - yMin || 1)) * innerH;

      const ns = "http://www.w3.org/2000/svg";
      function append(tag, attrs, parent = svg) {
        const el = document.createElementNS(ns, tag);
        Object.entries(attrs).forEach(([k, v]) => el.setAttribute(k, v));
        parent.appendChild(el);
        return el;
      }

      append("rect", { x: 0, y: 0, width, height, fill: "transparent" });

      for (let i = 0; i <= 5; i += 1) {
        const y = padY + (innerH / 5) * i;
        append("line", { x1: padX, y1: y, x2: width - padX, y2: y, stroke: "rgba(142,162,198,0.12)" });
      }
      for (let i = 0; i <= 5; i += 1) {
        const x = padX + (innerW / 5) * i;
        append("line", { x1: x, y1: padY, x2: x, y2: height - padY, stroke: "rgba(142,162,198,0.08)" });
      }
      append("line", { x1: padX, y1: height - padY, x2: width - padX, y2: height - padY, stroke: "rgba(234,242,255,0.55)", "stroke-width": 1.3 });
      append("line", { x1: padX, y1: padY, x2: padX, y2: height - padY, stroke: "rgba(234,242,255,0.55)", "stroke-width": 1.3 });

      const labels = subset.slice(0, 16);
      subset.forEach(row => {
        const cx = xScale(row.max_drawdown_pct);
        const cy = yScale(row.win_rate_pct);
        const pf = Number.isFinite(Number(row.profit_factor)) ? Number(row.profit_factor) : 8;
        const r = 4 + Math.min(pf, 8) * 1.6;
        const group = append("g", {});
        const circle = append("circle", {
          cx,
          cy,
          r,
          fill: colorForQuality(Number(row.quality_score)),
          stroke: "rgba(255,255,255,0.55)",
          "stroke-width": 1.1,
        }, group);
        const title = document.createElementNS(ns, "title");
        title.textContent = `${row.symbol} | WR ${fmt.pct(row.win_rate_pct)} | PF ${fmt.pf(row.profit_factor)} | DD ${fmt.pct(row.max_drawdown_pct)} | RET ${fmt.pct(row.net_return_pct)}`;
        circle.appendChild(title);
      });

      labels.forEach(row => {
        const cx = xScale(row.max_drawdown_pct);
        const cy = yScale(row.win_rate_pct);
        const label = append("text", {
          x: cx + 8,
          y: cy - 8,
          fill: "#dfeaff",
          "font-size": 11,
          "font-family": "SFMono-Regular, Menlo, monospace"
        });
        label.textContent = row.symbol;
      });

      const axisLabels = [
        ["Drawdown absoluto %", width - padX - 130, height - 16],
        ["Winrate %", 14, padY - 14],
      ];
      axisLabels.forEach(([text, x, y]) => {
        const t = append("text", { x, y, fill: "#8ea2c6", "font-size": 11, "font-family": "SFMono-Regular, Menlo, monospace" });
        t.textContent = text;
      });

      [["0", padX, height - padY + 20], [xMax.toFixed(1), width - padX - 14, height - padY + 20], ["0", padX - 24, height - padY + 4], [yMax.toFixed(0), padX - 30, padY + 4]].forEach(([text, x, y]) => {
        const t = append("text", { x, y, fill: "#8ea2c6", "font-size": 11, "font-family": "SFMono-Regular, Menlo, monospace" });
        t.textContent = text;
      });
    }

    function renderTable(rows) {
      const visible = rows.slice(0, state.limit);
      els.tableCaption.textContent = `${rows.length} monedas cumplen el filtro actual. Mostrando ${visible.length} filas.`;
      if (!visible.length) {
        els.tableBody.innerHTML = `<tr><td colspan="15"><div class="empty-state">No hay monedas para mostrar con este filtro.</div></td></tr>`;
        return;
      }
      els.tableBody.innerHTML = visible.map(row => `
        <tr>
          <td><strong>${row.symbol}</strong></td>
          <td>${row.baseCoin}</td>
          <td>${row.trades}</td>
          <td>${row.wins}</td>
          <td>${row.losses}</td>
          <td class="${row.win_rate_pct >= 60 ? "good" : row.win_rate_pct >= 45 ? "warn" : "bad"}">${fmt.pct(row.win_rate_pct)}</td>
          <td class="${row.profit_factor >= 2 ? "good" : row.profit_factor >= 1.2 ? "warn" : "bad"}">${fmt.pf(row.profit_factor)}</td>
          <td class="${row.expectancy_pct >= 0 ? "good" : "bad"}">${fmt.pct(row.expectancy_pct)}</td>
          <td class="${row.max_drawdown_pct >= -8 ? "good" : row.max_drawdown_pct >= -15 ? "warn" : "bad"}">${fmt.pct(row.max_drawdown_pct)}</td>
          <td class="${row.net_return_pct >= 0 ? "good" : "bad"}">${fmt.pct(row.net_return_pct)}</td>
          <td class="${qualityClass(row.quality_score)}">${fmt.num(row.quality_score)}</td>
          <td>${badge(row.trend_state)}</td>
          <td>${badge(row.last_signal)}</td>
          <td>${fmt.price(row.last_close)}</td>
          <td>${fmt.dt(row.last_bar_utc)}</td>
        </tr>
      `).join("");
    }

    function renderAll() {
      renderOverview(state.filtered);
      renderFilteredStats(state.filtered);
      renderTop50(state.filtered);
      renderPlot(state.filtered);
      renderMapInsights(state.filtered);
      renderTable(state.filtered);
    }

    els.mainTabs.forEach(btn => btn.addEventListener("click", () => setMainTab(btn.dataset.tab)));
    els.scanNowBtn.addEventListener("click", triggerScan);
    [els.search, els.minTrades, els.minWinrate, els.minPf, els.maxDd, els.trendFilter, els.signalFilter, els.sortBy, els.limitRows].forEach(el => {
      el.addEventListener("input", applyFilters);
      el.addEventListener("change", applyFilters);
    });
    els.resetBtn.addEventListener("click", () => {
      els.search.value = "";
      els.minTrades.value = payload.scanner?.min_trades ?? 0;
      els.minWinrate.value = 0;
      els.minPf.value = 0;
      els.maxDd.value = 100;
      els.trendFilter.value = "ALL";
      els.signalFilter.value = "ALL";
      els.sortBy.value = "quality_score";
      els.limitRows.value = "50";
      applyFilters();
    });
    document.querySelectorAll("thead th").forEach(th => {
      th.addEventListener("click", () => {
        const key = th.dataset.key;
        if (!key) return;
        if (state.sortBy === key) {
          state.sortDir = state.sortDir === "asc" ? "desc" : "asc";
        } else {
          state.sortBy = key;
          state.sortDir = key === "symbol" || key === "baseCoin" || key === "trend_state" || key === "last_signal" ? "asc" : "desc";
          els.sortBy.value = key;
        }
        state.filtered = sortRows(state.filtered);
        renderAll();
      });
    });

    hero();
    applyFilters();
  </script>
</body>
</html>
"""
    return template.replace("__TITLE__", "Bitget Scanner Dashboard").replace("__DATA__", data_json)


def main() -> int:
    parser = argparse.ArgumentParser(description="Build local Bitget scanner dashboard")
    parser.add_argument("--csv", type=Path, default=None)
    parser.add_argument("--summary", type=Path, default=None)
    parser.add_argument("--output", type=Path, default=BASE_DIR / "bitget_scanner_dashboard.html")
    args = parser.parse_args()

    csv_path = args.csv or latest_file("bitget_gaussian_scan_*.csv")
    if args.summary:
        summary_path = args.summary
    else:
        match = re.search(r"(\\d{8}_\\d{6})", csv_path.name)
        summary_path = None
        if match:
            candidates = sorted(
                path for path in BASE_DIR.glob(f"bitget_gaussian_scan_*_{match.group(1)}.json")
                if not path.name.endswith("_errors.json")
            )
            summary_path = candidates[-1] if candidates else None
        if summary_path is None:
            summary_path = latest_file("bitget_gaussian_scan_*.json")
        if not summary_path.exists():
            summary_path = latest_file("bitget_gaussian_scan_*.json")

    rows = load_rows(csv_path)
    summary = load_summary(summary_path)
    payload = make_payload(rows, summary, csv_path, summary_path)
    html = build_html(payload)
    args.output.write_text(html, encoding="utf-8")
    print(json.dumps({
        "output": str(args.output),
        "csv": str(csv_path),
        "summary": str(summary_path),
        "rows": len(rows)
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
