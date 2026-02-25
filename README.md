# Samsung Members Scraper + AI Classifier Pipeline

A two-stage data pipeline that scrapes Samsung Members forum posts and classifies them using an LLM/API-based classifier.

This project was built to automate the collection and labeling of Samsung Members posts for downstream analysis (e.g., issue categorization, topic tracking, and QA of scraped content).

---

## Overview

The pipeline is split into two scripts:

1. **`scraper.py`**  
   Scrapes Samsung Members listing pages and post detail pages, then exports structured data to Excel.

2. **`llmclassifier.py`**  
   Reads the scraped Excel output, prepares text fields, and runs an AI API classifier to generate structured labels.

---

## Pipeline Flow


```text
Samsung Members pages
        ↓
   scraper.py (Selenium)
        ↓
 Structured Excel output (.xlsx)
        ↓
 llmclassifier.py (OpenAI API)
        ↓
 Classified / enriched Excel output (.xlsx)
```
## Sample Output Schema

Example fields in the scraper output include:

- `Title`
- `Snippet`
- `FullText`
- `Replies`
- `RepliesCount`
- `AuthorName`

The classifier script reads the scraper output, selects a text field (or fallback combination), and appends classification labels / metadata to a new Excel output.



