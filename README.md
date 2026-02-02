# GF-Screening - Gesellschafterstruktur-Analyse

Automatisierte Analyse von GmbH-Gesellschafterstrukturen aus Dealfront-Listen.
Identifiziert Firmen mit ≤2 natürlichen Personen als Gesellschafter.

## Voraussetzungen

- Python 3.10+
- Google Chrome (für Selenium)
- Dealfront-Export mit Registernummern

## Installation

```bash
cd C:\Projekte\inqu\gf-screening

# Virtual Environment erstellen
python -m venv venv
venv\Scripts\activate  # Windows
# source venv/bin/activate  # Linux/Mac

# Dependencies installieren
pip install -r requirements.txt
```

## Verwendung

### 1. CSV importieren

```bash
cd src
python pipeline.py import "../dealfront_export.csv"
```

**Erwartete CSV-Spalten:**
- `Firma` oder `Firmenname` - Name der GmbH
- `Ort` oder `Stadt` - Firmensitz
- `Registernummer` oder `HRB` - Handelsregisternummer (z.B. "HRB 12345 B")

### 2. Gesellschafterlisten herunterladen

```bash
# Alle herunterladen (bei 2700 Firmen: ~50h)
python pipeline.py download

# Nur 10 zum Testen
python pipeline.py download --limit 10
```

**Hinweis:** Rate-Limit von 55 Abrufen/Stunde wird automatisch eingehalten.

### 3. PDFs parsen

```bash
python pipeline.py parse
```

### 4. Ergebnisse exportieren

```bash
python pipeline.py export
```

Output: `output/qualified_leads_YYYYMMDD_HHMMSS.csv`

### 5. Status prüfen

```bash
python pipeline.py stats
```

### Komplette Pipeline auf einmal

```bash
python pipeline.py run "../dealfront_export.csv" --limit 100
```

## Verzeichnisstruktur

```
gf-screening/
├── src/
│   ├── models.py          # Datenmodelle + SQLite
│   ├── dk_downloader.py   # Selenium-Scraper
│   ├── pdf_parser.py      # PDF-Parsing
│   └── pipeline.py        # Orchestrierung
├── pdfs/                  # Heruntergeladene PDFs
├── data/
│   └── gesellschafter.db  # SQLite-Datenbank
├── output/                # Exportierte CSVs
├── requirements.txt
└── README.md
```

## Einzelne Module testen

### PDF-Parser testen

```bash
python pdf_parser.py "../pdfs/HRB_12345_gesellschafterliste.pdf"
```

### Downloader testen (mit GUI)

```bash
python dk_downloader.py "HRB 12345" "Berlin"
```

## Laufzeit-Kalkulation

| Firmen | Download-Zeit | Parsing |
|--------|---------------|---------|
| 100    | ~2h           | <1min   |
| 1000   | ~18h          | ~5min   |
| 2700   | ~50h          | ~15min  |

**Empfehlung:** Als Nacht-Batch laufen lassen.

## Troubleshooting

### "Chrome not found"
```bash
# Chrome-Pfad manuell setzen in dk_downloader.py
options.binary_location = "C:/Program Files/Google/Chrome/Application/chrome.exe"
```

### Rate-Limit-Fehler
Die Pipeline hält automatisch 65 Sekunden zwischen Abrufen ein.
Bei Blockierung: 1h warten und mit `--resume` fortsetzen.

### PDF-Parsing-Fehler
- Niedrige Konfidenz (<0.5): Manuell prüfen
- OCR-PDFs: Nicht unterstützt (nur Text-PDFs)

## Rechtliche Hinweise

- Gesellschafterlisten sind seit 01.08.2022 kostenlos auf handelsregister.de
- Max. 60 Abrufe/Stunde laut Nutzungsordnung (Pipeline nutzt 55/h)
- Nur für B2B-Vertriebszwecke verwenden
