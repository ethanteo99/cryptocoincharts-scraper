cryptocoincharts-scraper
========================

Python-based scraper for exchange prices and volumes from cryptocoincharts.info.

Installation
=============

a) Make sure required python packages are installed

```
pip install cssselect lxml psycopg2 requests
```

b) Create tables in target PostgreSQL DB (see sql/)

c) Create .pgpass file in top-level of this directory containing connection info to the DB from previous step. Use the following format (9.1):

http://www.postgresql.org/docs/9.1/static/libpq-pgpass.html

d) Create "data" folder within the application folder, or change the saveToFile method in scrape.py to point to a different data directory.

Usage
=====

Simply run "python scrape.py".
