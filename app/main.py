"""
'yafi_fetch' master

1.  This script download the instrument list from SGX.
2.  It then parses the list for tickers.
3.  Tickers are then written to the 'yafi_fetch' queue.
"""

import json
import logging

from datetime import datetime
from os import path
from urllib.parse import quote
from urllib.request import urlopen as url_open
from urllib.request import Request as url_request

import pika

from logger import Logger
from program_arguments import get_settings_from_arguments

# 
# financial_instrument
# └───fetch.yahoo-finance.price
#     └───fetch_yahoo_finance_price

EXCHANGE_NAME = 'financial_instrument'
ROUTING_KEY = "yafi.fetch"
QUEUE_NAME = 'yafi_fetch'


def setup_logging():
    logging.getLogger('pika').setLevel(logging.WARNING)
    log = Logger()
    return log


def blacklist(ticker):
    logging.warn(f"TODO: Blacklist ticker {ticker}.")


# def fetch_chart_json_from_yafi(yami_ticker, range='max', granularity='1mo'):
#     app_path = ''
#     logging.info(f"Fetching chart data from YAFI: {yami_ticker}")
#     data_file_name = f'{yami_ticker}-{range}-{granularity}.json'
#     out_file_path = path.join(app_path, 'data-dump', data_file_name)
#     if path.exists(out_file_path):
#         return True # Skip
#     # There are a couple of URLs used to get the SGX data from Yahoo Finance.
#     # https://query1.finance.yahoo.com/v8/finance/chart/BN4.SI
#     api_url = f"https://query1.finance.yahoo.com/v8/finance/chart/{yami_ticker}?range={range}&granularity={granularity}"
#     request = url_request(api_url)
#     try:
#         with url_open(request) as response:
#             response_data = response.read().decode("utf-8")
#         save_price_data_to_file(data_file_name, json_data)
#         json_data = json.loads(response_data)
        
#         return True
#     except Exception:
#         return False


def save_price_data_to_file(ticker, data):
    FILE_DATETIME = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    FILE_NAME = f'{ticker}-{FILE_DATETIME}.json'
    SAVE_FILE_PATH = path.join(output_path, FILE_NAME)
    with open(SAVE_FILE_PATH, 'w', encoding='utf-8') as out_file:
        out_file.write(data)
    log.info("Price data saved", source="save_price_data_to_file()", event="save", data_length=len(data), ticker=ticker)

def fetch_price_data(ticker):
    ric_suffix = "SI"
    yami_ticker = f"{ticker}.{ric_suffix}"
    data_range='max'
    data_granularity='1mo'
    api_url = f"https://query1.finance.yahoo.com/v8/finance/chart/{yami_ticker}?range={data_range}&granularity={data_granularity}"
    request = url_request(api_url)
    try:
        with url_open(request) as response:
            log.info("Price data fetched", source="fetch_price_data()", event="response", status=response.status, ticker=ticker)
            response_data = response.read().decode("utf-8")
        save_price_data_to_file(ticker, response_data)
        return response_data
    except Exception:
        return None


def publish_parsed_price_data(json_data):
    log.info("TODO: publish_parsed_price_data")


def parse_and_publish_price_data(response_data):
    try:
        json_data = json.loads(response_data)
        publish_parsed_price_data(json_data)
    except Exception:
        return None


def process_message(channel, method, properties, body):
    log.info("Received message")
    ticker = body.decode('utf-8')
    response_data = fetch_price_data(ticker)
    if response_data is None:
        blacklist(ticker)
    parse_and_publish_price_data(response_data)
    # channel.basic_ack(delivery_tag=method.delivery_tag)
    log.info("Message processed", source="program", event="processed", content=ticker)


def setup_rabbit_mq(channel):
    channel.queue_declare(
        queue=QUEUE_NAME, 
        durable=True)

    channel.basic_consume(
        queue=QUEUE_NAME, 
        on_message_callback=process_message)
    

def listen_for_tickers(url_parameters):
    with pika.BlockingConnection(url_parameters) as connection, connection.channel() as channel:
        setup_rabbit_mq(channel)
        log.info("Listen to queue", source="program", event="listen", target=QUEUE_NAME)
        channel.start_consuming()


if __name__ == "__main__":
    log = setup_logging()
    (url_parameters, database_settings, output_path) = get_settings_from_arguments()
    listen_for_tickers(url_parameters)
    log.info("Program complete", source="program", event="complete")
