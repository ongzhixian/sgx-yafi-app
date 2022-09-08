"""
'yafi_fetch' master

1.  This script download the instrument list from SGX.
2.  It then parses the list for tickers.
3.  Tickers are then written to the 'yafi_fetch' queue.
"""

import json
import logging
import sys

from datetime import datetime
from os import path
from urllib.parse import quote
from urllib.request import urlopen as url_open
from urllib.request import Request as url_request
import re

import pika

from logger import Logger
from data_providers import MySqlDataProvider
from program_arguments import get_settings_from_arguments


EXCHANGE_NAME = 'financial_instrument'
ROUTING_KEY = "yafi.fetch"
QUEUE_NAME = 'yafi_fetch'

def setup_logging():
    logging.getLogger('pika').setLevel(logging.WARNING)
    log = Logger()
    return log

def setup_rabbit_mq(channel):
    channel.exchange_declare(
        exchange=EXCHANGE_NAME, 
        exchange_type='topic')
        
    channel.queue_declare(
        queue=QUEUE_NAME, 
        durable=True)
    
    channel.queue_bind(
        exchange=EXCHANGE_NAME, 
        routing_key=ROUTING_KEY,
        queue=QUEUE_NAME)


def publish_tickers(url_parameters, ticker_list):

    with pika.BlockingConnection(url_parameters) as connection, connection.channel() as channel:

        setup_rabbit_mq(channel)

        for ticker in ticker_list:

            channel.basic_publish(
                exchange=EXCHANGE_NAME, 
                routing_key=ROUTING_KEY, 
                body=ticker,
                properties=pika.BasicProperties(
                    delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
                ))

            log.info(f"Publish {ticker}", event="publish", type="ticker", target=ticker)
        

def get_instrument_code_list(sgx_isin_data_rows):
    return ['BN4', 'C09']
    instrument_code_list = [ x[1] for x in sgx_isin_data_rows ]
    return instrument_code_list
    

def parse_to_data_rows(sgx_isin_data):
    """Parse contents of the file into records of instrument (instrument_list)"""
    market_identifier_id = 'XSES'
    ticker_list = []
    instrument_list = []
    sgx_isin_layout = r"(?P<name>.{50})(?P<status>.{10})(?P<isin>.{20})(?P<code>.{10})(?P<counter>.+)"
    
    isin_line_list = sgx_isin_data.splitlines()
    for line in isin_line_list[1:]:
        if len(line.strip()) <= 0:
            continue
        
        match_result = re.match(sgx_isin_layout, line)
        if match_result is None:
            continue
        
        code = match_result.group('code').strip()
        ticker_list.append(code)
        instrument_list.append(
            (
                market_identifier_id, 
                match_result.group('code').strip(),
                match_result.group('name').strip(),
                match_result.group('counter').strip(),
                match_result.group('isin').strip()
            )
        )
    return instrument_list


def save_sgx_isin_to_file(data):
    FILE_DATETIME = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    FILE_NAME = f'sgx-isin-{FILE_DATETIME}.txt'
    SAVE_FILE_PATH = path.join(output_path, FILE_NAME)
    with open(SAVE_FILE_PATH, 'w', encoding='utf-8') as out_file:
        out_file.write(data)


def download_sgx_instrument_list(output_path=None):
    log.info("Download SGX instrument list", source="program", event="download", target="SGX instrument list")
    # Save downloaded file
    today = datetime.today().strftime('%d %b %Y')
    logging.info(f"Getting ISINs from SGX for {today}")
    url = f'https://links.sgx.com/1.0.0/isin/1/{quote(today)}'
    
    request = url_request(
        url, 
        data=None, 
        headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:103.0) Gecko/20100101 Firefox/103.0'
        }
    )

    with url_open(request) as response:
        sgx_isin_data = response.read().decode("utf-8")

    # TODO: Some download data verification would be nice

    if output_path is not None:
        save_sgx_isin_to_file(sgx_isin_data)
    
    return sgx_isin_data


def store_sgx_isin_to_database(sgx_isin_data_rows):
    mysql = MySqlDataProvider(database_settings['financial'])
    sql = """
INSERT INTO instrument (market_identifier_id, code, name, counter, isin)
SELECT  %s      AS 'market_identifier_id'
        , %s    AS 'code'
        , %s    AS 'name'
        , %s    AS 'counter'
        , %s    AS 'isin' 
FROM    (SELECT 1) a
WHERE NOT EXISTS (SELECT 1 FROM instrument WHERE market_identifier_id = %s AND code = %s)
LIMIT 1;
"""
    data_rows = [(x[0], x[1], x[2], x[3], x[4], x[0], x[1]) for x in sgx_isin_data_rows]
    (rows_affected, errors) = mysql.execute_batch(sql, data_rows)
    log.info("Rows affected {rows_affected}")


def get_blacklisted_code_list():
    """Fetch codes of blacklisted instruments"""
    mysql = MySqlDataProvider(database_settings['financial'])
    sql = """
SELECT code FROM instrument WHERE exclusion_id IS NOT NULL AND market_identifier_id = %s;
"""
    blacklisted_record_list = mysql.fetch_record_set(sql, ('XSES',))
    blacklisted_code_list = [x[0] for x in blacklisted_record_list]
    return blacklisted_code_list


def remove_blacklisted_instrument_codes(instrument_code_list):
    blacklisted_code_list = get_blacklisted_code_list()
    filtered_instrument_code_list = [code for code in instrument_code_list if code not in blacklisted_code_list]
    return filtered_instrument_code_list


if __name__ == "__main__":
    log = setup_logging()
    (url_parameters, database_settings, output_path) = get_settings_from_arguments()
    
    # sgx_isin_data = download_sgx_instrument_list(output_path)
    # sgx_isin_data_rows = parse_to_data_rows(sgx_isin_data)

    # # Part 1 -- Store to database part
    # store_sgx_isin_to_database(sgx_isin_data_rows)

    # # Part 2 -- Publishing
    # instrument_code_list = get_instrument_code_list(sgx_isin_data_rows)
    # filtered_instrument_code_list = remove_blacklisted_instrument_codes(instrument_code_list)
    # publish_tickers(url_parameters, filtered_instrument_code_list)
    
    log.info("Program complete", source="program", event="complete")
