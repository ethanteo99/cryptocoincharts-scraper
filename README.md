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

Usage
=====

Simply run "python scraper.py".
