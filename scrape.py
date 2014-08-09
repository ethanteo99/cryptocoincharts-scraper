"""Core scraper for prive volume data from cryptocoincharts.info."""
import codecs
import cryptocoincharts
import logging
import os
import pg
import sys
import time
import traceback


# Helper for file writing
def writeToFile(content, prefix, extension):
    """Write data to file."""
    f = codecs.open("{0}/data/{1}_{2}.{3}".format(
        os.path.dirname(os.path.abspath(__file__)),
        prefix, int(time.time()), extension), 'w', 'utf-8')
    f.write(content)
    f.close()

# Establish database connection
cursor = pg.dictCursor()

# Set logging level
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s:%(message)s',
    datefmt='%m/%d/%Y %I:%M:%S %p')

# Pull in the list of exchanges
logging.info("Starting scrape of exchange list.")
exchangesHtml = cryptocoincharts.requestExchanges()
writeToFile(exchangesHtml, "exchanges", "html")
exchanges = cryptocoincharts.parseExchanges(exchangesHtml)
logging.info("Finished scrape of exchange list.")

# Compile a list of every required exchange and currency pair
logging.info("Starting scrape of individual exchanges.")
exchangePairs = []
for exchange in exchanges:
    logging.info("Starting scrape of exchange {0}.".format(
        exchange["short_name"]))
    exchangeHtml = cryptocoincharts.requestExchange(exchange["short_name"])
    writeToFile(
        exchangeHtml,
        "exchange_{0}".format(exchange["short_name"]),
        "html"
    )
    exchangeSummary, pairs = cryptocoincharts.parseExchange(exchangeHtml)
    for pair in pairs:
            exchangePairs.append({
                "source": pair["source"],
                "sink": pair["sink"],
                "exchange": exchange["short_name"]
            })
    logging.info("Finished scrape of exchange {0}.".format(
        exchange["short_name"]))
logging.info("Finished scrape of indivdual exchanges.")

# exchangePairs = [
#     {'source': 'usd', 'sink': 'btc', 'exchange': 'btc-e'},
#     {'source': 'usd', 'sink': 'ltc', 'exchange': 'btc-e'}
# ]

# Download information for every exchange and currency pair
logging.info("Starting scrape of price volume information")
priceVolumes = []
cursor.execute("""SELECT
        CONCAT(exchange, '-', source, '-', sink) AS "exchange_pair",
        MAX(hour) AS "last_hour"
    FROM exchange_pair_hour
    GROUP BY exchange, source, sink
    ORDER BY exchange, source, sink""")
rows = cursor.fetchall()
priceVolumesLatest = dict(
    [(row["exchange_pair"], row["last_hour"]) for row in rows]
)
for exchangePair in exchangePairs:
    # Find out whether we want data over all time or only the last several days
    exchangePairCompact = "{0}-{1}-{2}".format(
        exchangePair["exchange"],
        exchangePair["source"],
        exchangePair["sink"])
    if exchangePairCompact in priceVolumesLatest:
        logging.info("Previous data exists. \
            Starting scrape for 10 days of price volume info for {0}".format(
            exchangePairCompact))
        pvTime = "10d"
    else:
        logging.info("No previous data exists. \
            Starting scrape for all price volume info for {0}".format(
            exchangePairCompact))
        pvTime = "alltime"
    # Scrape the actual data
    priceVolumeParams = [
        exchangePair["source"], exchangePair["sink"],
        exchangePair["exchange"], pvTime, "1h"
    ]
    try:
        priceVolumeJsonDump = cryptocoincharts.requestPriceVolume(
            *priceVolumeParams)
    except Exception as e:
        priceVolumeStr = "-".join(priceVolumeParams)
        print '-'*60
        print "Could not request URL for price volume {0}:".format(
            priceVolumeStr)
        print traceback.format_exc()
        print '-'*60
        logging.info("Could not request URL for price volume {0}:".format(
            priceVolumeStr))
        continue
    writeToFile(
        priceVolumeJsonDump,
        "price_volume_{0}".format("_".join(priceVolumeParams)),
        "json"
    )
    priceVolume = cryptocoincharts.parsePriceVolume(
        priceVolumeJsonDump, exchangePair["source"],
        exchangePair["sink"], exchangePair["exchange"])
    pg.loadPriceVolume(priceVolume)
