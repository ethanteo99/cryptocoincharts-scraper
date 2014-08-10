"""Module for storing cryptocoincharts data in the database."""
import cryptocoincharts
import datetime
from decimal import Decimal
import os
import psycopg2 as pg2
import psycopg2.extras as pg2ext
import random
import unittest

# Configuration variables
batchLimit = 1000
targetTable = "exchange_pair_hour"

# Pull in postgres configuration information
# Pull in postgres configuration information
dbcFile = open(
    "{0}/.pgpass".format(os.path.dirname(os.path.abspath(__file__))),
    'r')
dbcRaw = dbcFile.readline().strip().split(':')
dbcParams = {
    'database': dbcRaw[2],
    'user': dbcRaw[3],
    'password': dbcRaw[4],
    'host': dbcRaw[0],
    'port': dbcRaw[1]
}
dbcFile.close()

# Connection variable
conn = None


def connect():
    """Connect to the database."""
    global conn
    if conn is not None:
        return conn
    else:
        conn = pg2.connect(**dbcParams)
        return conn


def cursor():
    """"Pull a cursor from the connection."""
    return connect().cursor()


def dictCursor():
    """"Pull a dictionary cursor from the connection."""
    return connect().cursor(cursor_factory=pg2ext.RealDictCursor)


def loadPriceVolume(data):
    """Load price volume data."""
    cursor = dictCursor()

    # Create staging table
    stagingTable = "{0}_{1}".format(
        targetTable, str(int(pow(10, random.random()*10))).zfill(10))
    cursor.execute("""CREATE TABLE {0} (LIKE {1}
        )""".format(stagingTable, targetTable))

    # Move data into staging table
    batchCount = 0
    while batchCount*batchLimit < len(data):
        cursor.executemany("""
            INSERT INTO {0} (
                exchange, source, sink, hour,
                price_low, price_25th_percentile,
                price_75th_percentile, price_high,
                price_median, price_ema20, volume,
                field_7, field_8)
            VALUES (
                %(exchange)s,
                %(source)s,
                %(sink)s,
                %(hour)s,
                %(price_low)s,
                %(price_25th_percentile)s,
                %(price_75th_percentile)s,
                %(price_high)s,
                %(price_median)s,
                %(price_ema20)s,
                %(volume)s,
                %(field_7)s,
                %(field_8)s
            )
            """.format(stagingTable),
            data[(batchCount*batchLimit):((batchCount+1)*batchLimit)])
        batchCount += 1

    # Delete out rows with content similar to what we are about to insert
    cursor.execute("""
        DELETE FROM {0} as tgt
        USING {1} as stg
        WHERE tgt.exchange = stg.exchange
        AND tgt.source = stg.source
        AND tgt.sink = stg.sink
        AND tgt.hour = stg.hour""".format(targetTable, stagingTable))

    # Insert the new data into the target table
    cursor.execute("""
        INSERT INTO {0}
        (SELECT *
        FROM {1})""".format(targetTable, stagingTable))

    # Drop the staging table
    cursor.execute("""
        DROP TABLE {0}""".format(stagingTable))

    # Commmit the transaction
    cursor.execute("COMMIT")

    # Return
    return True


class PgTest(unittest.TestCase):

    """Testing suite for pg module."""

    def setUp(self):
        """Setup tables for test."""
        # Swap and sub configuration variables
        global targetTable
        self.targetTableOriginal = targetTable
        targetTable = "{0}_test".format(self.targetTableOriginal)
        global batchLimit
        self.batchLimitOriginal = batchLimit
        batchLimit = 1000

        # Create test tables
        cursor = dictCursor()
        cursor.execute("""CREATE TABLE IF NOT EXISTS
            {0} (LIKE {1} INCLUDING ALL)""".format(
            targetTable, self.targetTableOriginal))
        cursor.execute("""COMMIT""")

    def tearDown(self):
        """Teardown test tables."""
        # Drop test tables
        global targetTable
        cursor = dictCursor()
        cursor.execute("""DROP TABLE IF EXISTS
            {0}""".format(targetTable))
        cursor.execute("""COMMIT""")

        # Undo swap / sub
        targetTable = self.targetTableOriginal
        global batchLimit
        batchLimit = self.batchLimitOriginal

    def testLoadPriceVolumeLogic(self):
        """Test loadPriceVolume function - Part 1."""
        # Load data
        dataFirst = [
            {
                'price_median': 614.243,
                'price_75th_percentile': 615.487,
                'hour': datetime.datetime(2014, 7, 22, 15, 0),
                'exchange': 'btc-e',
                'price_25th_percentile': 612.999,
                'volume': 127.469,
                'source': 'usd',
                'price_ema20': 614.49802606891,
                'sink': 'btc',
                'field_8': 0,
                'price_high': 615.5,
                'field_7': 78205.8,
                'price_low': 612.212
            },
            {
                'price_median': 614.219,
                'price_75th_percentile': 615.4,
                'hour': datetime.datetime(2014, 7, 22, 16, 0),
                'exchange': 'btc-e',
                'price_25th_percentile': 613.038,
                'volume': 51.1461,
                'source': 'usd',
                'price_ema20': 614.58392834806,
                'sink': 'btc',
                'field_8': 0,
                'price_high': 615.5,
                'field_7': 31448.4,
                'price_low': 612.5
            }
        ]
        loadPriceVolume(dataFirst)
        dataSecond = [
            {
                'price_median': 614.1755,
                'price_75th_percentile': 614.502,
                'hour': datetime.datetime(2014, 7, 22, 16, 0),
                'exchange': 'btc-e',
                'price_25th_percentile': 613.849,
                'volume': 65.55,
                'source': 'usd',
                'price_ema20': 614.57612564825,
                'sink': 'btc',
                'field_8': 0,
                'price_high': 615.989,
                'field_7': 40347.2,
                'price_low': 613.313
            },
            {
                'price_median': 614.996,
                'price_75th_percentile': 615.49,
                'hour': datetime.datetime(2014, 7, 22, 17, 0),
                'exchange': 'btc-e',
                'price_25th_percentile': 614.502,
                'volume': 5.34896,
                'source': 'usd',
                'price_ema20': 614.6631613008,
                'sink': 'btc',
                'field_8': 0,
                'price_high': 615.5,
                'field_7': 3288.51,
                'price_low': 614.5
            }
        ]
        loadPriceVolume(dataSecond)

        # Check content
        dataExpected = [
            {
                'price_median': Decimal('614.243'),
                'price_75th_percentile': Decimal('615.487'),
                'hour': datetime.datetime(2014, 7, 22, 15, 0),
                'exchange': 'btc-e',
                'price_25th_percentile': Decimal('612.999'),
                'volume': Decimal('127.469'),
                'source': 'usd',
                'price_ema20': Decimal('614.49802606891'),
                'sink': 'btc',
                'field_8': Decimal('0'),
                'price_high': Decimal('615.5'),
                'field_7': Decimal('78205.8'),
                'price_low': Decimal('612.212')
            },
            {
                'price_median': Decimal('614.1755'),
                'price_75th_percentile': Decimal('614.502'),
                'hour': datetime.datetime(2014, 7, 22, 16, 0),
                'exchange': 'btc-e',
                'price_25th_percentile': Decimal('613.849'),
                'volume': Decimal('65.55'),
                'source': 'usd',
                'price_ema20': Decimal('614.57612564825'),
                'sink': 'btc',
                'field_8': Decimal('0'),
                'price_high': Decimal('615.989'),
                'field_7': Decimal('40347.2'),
                'price_low': Decimal('613.313')
            },
            {
                'price_median': Decimal('614.996'),
                'price_75th_percentile': Decimal('615.49'),
                'hour': datetime.datetime(2014, 7, 22, 17, 0),
                'exchange': 'btc-e',
                'price_25th_percentile': Decimal('614.502'),
                'volume': Decimal('5.34896'),
                'source': 'usd',
                'price_ema20': Decimal('614.6631613008'),
                'sink': 'btc',
                'field_8': Decimal('0'),
                'price_high': Decimal('615.5'),
                'field_7': Decimal('3288.51'),
                'price_low': Decimal('614.5')
            }
        ]

        # Verify
        cursor = dictCursor()
        cursor.execute("""SELECT *
            FROM {0}""".format(targetTable))
        rows = cursor.fetchall()
        self.assertEqual(rows, dataExpected)

    def testLoadPriceVolumePractical(self):
        """Test loadPriceVolume function - Part 2."""
        fileString = "{0}/example/price_volume_usd_btc_btc-e_alltime_1h.json"
        f = open(fileString.format(
            os.path.dirname(os.path.abspath(__file__))), 'r')
        jsonDump = f.read()
        f.close()
        data = cryptocoincharts.parsePriceVolume(
            jsonDump, "usd", "btc", "btc-e")
        loadPriceVolume(data)

if __name__ == "__main__":
    unittest.main()
