from dagster import asset

import json, os, requests
from . import constants
from dateutil import parser

from influxdb_client import InfluxDBClient, Point, WriteOptions
from influxdb_client.client.write_api import SYNCHRONOUS

@asset
def bot_exchange_rates_file():
    """
    The raw json files for the bank of Thailand (bot) exchange rates dataset. Sourced from https://www.bot.or.th/en/statistics/exchange-rate.html.
    """
    year_to_fetch = '2023'
    month_to_fetch = '2023-02'
    date_to_fetch = '2023-02-01'
    r = requests.get(constants.BOT_EXCHANGE_RATE_URL.format(date_to_fetch))
    resp = r.json()

    output_path = os.path.join(constants.BOT_EXCHANGE_RATE_FILE_PATH, year_to_fetch, month_to_fetch)
    if not os.path.exists(output_path):
        os.makedirs(output_path)

    with open(os.path.join(output_path, date_to_fetch + '.json'), "w") as output_file:
        json.dump(resp, output_file, ensure_ascii=False, indent=4)


@asset(deps=["bot_exchange_rates_file"])
def bot_exchange_rates():
    """
    The raw bot exchange rates dataset, loaded into a DuckDB database
    """
    year_to_fetch = '2023'
    month_to_fetch = '2023-02'
    date_to_fetch = '2023-02-01'
    
    input_path = os.path.join(constants.BOT_EXCHANGE_RATE_FILE_PATH, year_to_fetch, month_to_fetch)
    points = []
    with open(os.path.join(input_path, date_to_fetch + '.json'), "r") as json_file:
        data = json.load(json_file)
        if "responseContent" in data:
            content = data["responseContent"]
            for d in content:
                point = Point("currency")
                point.tag("key", d["currency_id"])
                point.time(parser.parse(d["period"]))
                if d["buying_sight"] not in ('-', 'N/A'):
                    point.field("buying_sight", float(d["buying_sight"]))
                if d["buying_transfer"] not in ('-', 'N/A'):
                    point.field("buying_transfer", float(d["buying_transfer"]))
                if d["selling"] not in ('-', 'N/A'):
                    point.field("selling", float(d["selling"]))
                points.append(point)

    with InfluxDBClient(
        url=constants.INFLUXDB_URL,
        token=constants.INFLUXDB_TOKEN,
        org=constants.INFLUXDB_ORG,
        timeout=300_000
    ) as client:
        # Write script
        write_api = client.write_api(write_options=SYNCHRONOUS)
        write_api.write(bucket=constants.INFLUXDB_BUCKET, org=constants.INFLUXDB_ORG, record=points)