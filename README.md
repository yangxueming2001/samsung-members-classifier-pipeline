# Samsung Members Scraper + AI Classifier Pipeline

This project scrapes posts from Samsung Members and classifies them using an AI-based classifier pipeline.

## Features
- Scrapes post metadata (e.g., title, author, category, snippet)
- Cleans / preprocesses scraped data
- Runs AI API classification on posts
- Outputs structured results for downstream analysis

## Project Structure
- `scraper.py` — scraping logic for Samsung Members posts
- `classifier.py` — AI API classifier pipeline
- `requirements.txt` — Python dependencies

## Setup
1. Clone the repo
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
