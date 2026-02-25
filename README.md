# Samsung Members Scraper + AI Classifier Pipeline

This project is a two-stage pipeline for collecting and classifying Samsung Members forum posts:

1. **`scraper.py`** — scrapes Samsung Members post metadata/content into Excel
2. **`llmclassifier.py`** — classifies scraped posts using an AI API and writes enriched output

## Project Files

- `scraper.py` — Selenium scraper for Samsung Members pages/posts
- `llmclassifier.py` — LLM/API-based classification pipeline
- `requirements.txt` — Python dependencies
- `.gitignore` — ignores secrets, cache files, and generated outputs

## Features

- Scrapes post data such as title, snippet, full text, replies, and author fields
- Supports multiple Samsung Members market endpoints (configurable in scraper settings)
- Runs AI-based classification on scraped post text
- Writes results to Excel for downstream analysis

## Setup

### 1) Install dependencies
```bash
pip install -r requirements.txt
