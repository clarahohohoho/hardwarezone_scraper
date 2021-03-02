# hardwarezone_scraper

An automated scraper that scrapes the hardwarezone forums.

## Parameters

Currently the scraper will stop scraping once the dataframe reaches 1 million entries.
Scraper will automatically save df as 'corpus3.csv' at every 100 entries.

## To use

1. Install dependencies

```
pip install -r requirements.txt
```

2. Run scrapper

```
python scrap.py
```

