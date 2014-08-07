"""Module for scraping and parsing data from cryptocoincharts.info."""
from datetime import date
from datetime import datetime
import json
import logging
import lxml.html
import requests
import os
from random import random
import re
import time
import unittest

baseUrl = "http://www.cryptocoincharts.info"
countRequested = 0
interReqTime = 2
lastReqTime = None


def _request(urlPostfix, params={}):
    """Private method for requesting an arbitrary query string."""
    global countRequested
    global lastReqTime
    if lastReqTime is not None and time.time() - lastReqTime < interReqTime:
        timeToSleep = random()*(interReqTime-time.time()+lastReqTime)*2
        logging.info("Sleeping for {0} seconds before request.".format(
            timeToSleep))
        time.sleep(timeToSleep)
    logging.info("Issuing request for the following payload: {0}".format(
        urlPostfix))
    r = requests.get("{0}/{1}".format(baseUrl, urlPostfix), params=params)
    lastReqTime = time.time()
    countRequested += 1
    if r.status_code == requests.codes.ok:
        return r.text
    else:
        raise Exception("Could not process request. \
            Received status code {0}.".format(r.status_code))


def requestExchanges():
    """Request list of exchanges."""
    return _request("v2/markets/info")


def parseExchanges(html):
    """Parse list of exchanges."""
    data = []
    exchangesRaw = lxml.html.fromstring(html).cssselect(
        "#tableMarkets > tbody > tr")
    for exchangeRaw in exchangesRaw:
        datum = {}
        columns = exchangeRaw.cssselect("td")
        for columnNum, column in enumerate(columns):
            if columnNum == 0:
                link = column.cssselect("a")[0]
                datum['name'] = link.text
                datum['url'] = link.attrib["href"]
                datum["short_name"] = link.attrib["href"].split("/")[-1]
            elif columnNum == 1:
                datum['last_update'] = column.attrib["data-sort-value"]
            elif columnNum == 2:
                datum['num_trading_pairs'] = column.attrib["data-sort-value"]
            elif columnNum == 3:
                datum['total_volume'] = column.attrib["data-sort-value"]
        data.append(datum)
    return data


def requestExchange(shortName):
    """Request information on a single exchange."""
    return _request("v2/markets/show/{0}".format(shortName))


def parseExchange(html):
    """Parse information for a single exchange."""
    data = []
    doc = lxml.html.fromstring(html).cssselect(".col-md-6")

    # Summary data
    summaryRows = doc[0].cssselect("table > tbody > tr")
    summary = {}
    for rowNum, summaryRow in enumerate(summaryRows):
        if rowNum == 0:
            summary["num_trading_pairs"] = int(
                summaryRow.cssselect("span")[0].text)
        elif rowNum == 1:
            volsRaw = summaryRow.cssselect("td")[1].text_content()
            volsRaw = volsRaw.strip().split("\n\t\t\t\t\t\t")
            for volCount, volRaw in enumerate(volsRaw):
                volParts = volRaw.strip().split(u"\xa0")
                summary["vol_{0}".format(volCount+1)] = float(
                    volParts[0].replace(',', ''))
                summary["vol_{0}_unit".format(volCount+1)] = volParts[1].lower(
                    )
        elif rowNum == 2:
            candidateDatetime = summaryRow.cssselect(
                "td")[1].text.split("<br />")[0].strip()
            if candidateDatetime == '':
                summary["last_updated"] = None
            else:
                summary["last_updated"] = datetime.strptime(
                    candidateDatetime, "%Y-%m-%d %H:%M:%S")
        elif rowNum == 3:
            summary["url"] = summaryRow.cssselect("a")[0].attrib["href"]

    # Pair Data
    pairRows = doc[1].cssselect("table > tbody > tr")
    pairs = []
    for pairRow in pairRows:
        columns = pairRow.cssselect("td")
        pair = {}
        for columnNum, column in enumerate(columns):
            if columnNum == 0:
                link = column.cssselect("a")[0]
                pair['name'] = link.text
                pair['url'] = link.attrib["href"]
                pair['source'] = link.attrib["href"].split("/")[-2]
                pair['sink'] = link.attrib["href"].split("/")[-3]
            elif columnNum == 1:
                pair['source_price'] = float(
                    column.text.split(' ')[0].replace(',', ''))
            else:
                raw = column.text.split(' ')[0].split(u"\xa0")
                rawVolume = raw[0]
                currency = raw[1].lower()

                if currency == pair['source']:
                    currencyType = "source"
                elif currency == pair['sink']:
                    currencyType = "sink"
                else:
                    currencyType = currency

                pair["{0}_volume".format(currencyType)] = float(
                    rawVolume.replace(',', ''))

                if currency == 'btc':
                    pair['btc_volume'] = float(rawVolume.replace(',', ''))

        pairs.append(pair)

    return summary, pairs


def requestPriceVolume(source, sink, exchange, time, resolution):
    """Request price / volume data for specific exchange & trading pair."""
    payload = {
        "pair": "{0}-{1}".format(sink, source),
        "market": exchange,
        "time": time,
        "resolution": resolution
    }
    return _request("v2/fast/period.php", payload)


def parsePriceVolume(jsonDump, source, sink, exchange):
    """Parse price / volume data for specific exchange & trading pair."""
    rows = json.loads(jsonDump)
    data = []
    for row in rows:
        datum = {}
        datum["source"] = source
        datum["sink"] = sink
        datum["exchange"] = exchange
        if len(row[0]) == 10:
            datum["date"] = date.strptime(row[0], "%Y-%m-%d")
        elif len(row[0]) == 13:
            datum["hour"] = datetime.strptime(row[0], "%Y-%m-%d %H")
        datum["price_low"] = row[1]
        datum["price_25th_percentile"] = row[2]
        datum["price_75th_percentile"] = row[3]
        datum["price_high"] = row[4]
        datum["price_median"] = row[5]
        datum["price_ema20"] = row[9]
        datum["volume"] = row[6]
        datum["field_7"] = row[7]
        datum["field_8"] = row[8]
        data.append(datum)
    return data


class CryptocoinchartsTest(unittest.TestCase):

    """Class for testing cryptocoincharts module."""

    def testRequestExchanges(self):
        """Test requestExchanges function."""
        html = requestExchanges()
        f = open("{0}/data/test_exchanges.html".format(os.path.dirname(
            os.path.abspath(__file__))), 'w')
        f.write(html)
        f.close()
        receivedTitle = lxml.html.fromstring(html).cssselect("title")[0].text
        self.assertEqual(receivedTitle, "List of all cryptocurrency exchanges")

    def testParseExchanges(self):
        """Test parseExchanges function."""
        f = open("{0}/example/exchanges.html".format(
            os.path.dirname(os.path.abspath(__file__))), 'r')
        html = f.read()
        f.close()
        data = parseExchanges(html)
        self.assertEqual(data[0]['name'], "Bitstamp")
        self.assertEqual(data[1]["last_update"], "1405977068")
        self.assertEqual(data[2]["url"], "/v2/markets/show/btcchina")
        self.assertEqual(data[2]["short_name"], "btcchina")
        self.assertEqual(data[4]["num_trading_pairs"], "389")
        self.assertEqual(data[5]["total_volume"], "635")

    def testRequestExchange(self):
        """Test requestExchange function."""
        html = requestExchange("btc-e")
        f = open("{0}/data/test_exchange_btc-e.html".format(os.path.dirname(
            os.path.abspath(__file__))), 'w')
        f.write(html)
        f.close()
        receivedTitle = lxml.html.fromstring(html).cssselect("title")[0].text
        expTitle = "BTC-e trading pairs and other informations and statistics"
        self.assertEqual(receivedTitle, expTitle)

    def testParseExchange(self):
        """Test parseExchange function."""
        f = open("{0}/example/exchange_btc-e.html".format(
            os.path.dirname(os.path.abspath(__file__))), 'r')
        html = f.read()
        f.close()
        summary, data = parseExchange(html)

        # Check summary data
        summaryExpected = {
            'num_trading_pairs': 23,
            'vol_1': 3814.05,
            'vol_1_unit': 'btc',
            'vol_2': 2364939.84,
            'vol_2_unit': 'usd',
            'vol_3': 1745690.65,
            'vol_3_unit': 'eur',
            'last_updated': datetime(2014, 7, 21, 23, 53, 4),
            'url': "https://btc-e.com/"
        }
        self.assertEqual(summary, summaryExpected)

        # Check pair data
        self.assertEqual(data[1]['name'], "LTC/USD")
        self.assertEqual(data[2]["source"], "btc")
        self.assertEqual(data[3]["sink"], "ppc")
        self.assertEqual(data[4]["source_price"], 23336)
        self.assertEqual(data[5]["source_volume"], 33743.07)
        self.assertEqual(data[6]["sink_volume"], 32378.09)
        self.assertEqual(data[7]["btc_volume"], 30.27)

    def testRequestPriceVolume(self):
        """Test requestPriceVolume function."""
        params = ["usd", "btc", "btc-e", "alltime", "1h"]
        jsonDump = requestPriceVolume(*params)
        f = open("{0}/data/test_price_volume_{1}.json".format(
            os.path.dirname(os.path.abspath(__file__)), "_".join(params)), 'w')
        f.write(jsonDump)
        f.close()
        # Make sure JSON is well-formed by trying to load it
        json.loads(jsonDump)

    def testParsePriceVolume(self):
        """Test parsePriceVolume."""
        fileString = "{0}/example/price_volume_usd_btc_btc-e_alltime_1h.json"
        f = open(fileString.format(
            os.path.dirname(os.path.abspath(__file__))), 'r')
        jsonDump = f.read()
        f.close()
        data = parsePriceVolume(jsonDump, "usd", "btc", "btc-e")

if __name__ == "__main__":
    unittest.main()
