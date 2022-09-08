import argparse
import json
from os import path, makedirs

import pika

from logger import Logger

log = Logger()

def get_argument_parser():
    # Setup ArgumentParser; # Program should take 3 arguments: .cloud-amqp.json .mysql.json output-path
    parser = argparse.ArgumentParser()
    # parser.add_argument("command", help="echo the string you use here")
    # parser.add_argument("-c", "--command", choices=[
    #     'dump-oanda', 
    #     'grab-sgx', 'dbg-sgx',
    #     'test'
    #     ], help="Some operation command")
    # parser.add_argument("-a", "--arguments", help="Some arguments to complement command")
    # parser.add_argument("-v", "--verbosity", type=int, help="increase output verbosity")
    # parser.add_argument("-v", "--verbosity", type=int, choices=[0, 1, 2], help="increase output verbosity")
    # parser.add_argument("-v", "--verbosity", action="count", default=0, help="increase output verbosity")
    # parser.add_argument("echo", help="echo the string you use here")
    # parser.add_argument("square", help="display a square of a given number", type=int)
    
    # Arguments we need:
    # output-path
    # cloud-amqp-config
    # database-config
    # Required arguments
    parser.add_argument("cloud-amqp-config", help="Location to cloud AMQP configuration (json) file")
    parser.add_argument("database-config", help="Location to database configuration (json) file")
    parser.add_argument("save-path", help="Folder location to save downloaded files")
    # Parameters starting with - or -- are usually considered optional; although this can be circumvent by adding required attribute
    # parser.add_argument("-s", "--save-path", help="Folder location to save downloaded files", required=True)
    # parser.add_argument("-a", "--cloud-amqp-config", help="Location to cloud AMQP configuration (json) file")
    # parser.add_argument("-d", "--database-config", help="Location to database configuration (json) file")
    return parser

def get_amqp_url_parameters(config_file_path):
            
    full_path = path.abspath(config_file_path)
    
    if not path.exists(full_path):
        log.error(f"Path {full_path} does not exists.")
        exit(2)
    
    try:
        with open(full_path, "r", encoding="utf-8") as in_file:
            json_data = json.load(in_file)
    except Exception as e:
        log.error(e)
        exit(3)
    
    if 'cloud_amqp' not in json_data \
        or 'armadillo' not in json_data['cloud_amqp'] \
        or'url' not in json_data['cloud_amqp']['armadillo']:
        log.error("Config file does not have structure [ cloud_amqp > armadillo > url ]")
        exit(4)
    
    cloud_amqp_url = json_data['cloud_amqp']['armadillo']['url']

    log.info("Cloud AMQP URL read", source="program", event="set", target="cloud amqp url")
    
    return pika.URLParameters(cloud_amqp_url)

def get_database_settings(config_file_path):
    
    full_path = path.abspath(config_file_path)
    
    if not path.exists(full_path):
        log.error(f"ERROR - output path does not exists {full_path}")
        exit(2)

    with open(full_path, 'r', encoding='utf-8') as in_file:
        mysql_settings = json.loads(in_file.read())

    return mysql_settings

def get_save_file_full_path(directory_path):

    full_path = path.abspath(directory_path)
    
    if not path.exists(full_path):
        try:
            makedirs(full_path)
        except Exception:
            log.error(f"ERROR - output path does not exists {full_path}")
            exit(2)

    return full_path


def get_settings_from_arguments():
    parser = get_argument_parser()
    args = vars(parser.parse_args())
    cloud_amqp_config_file_path = args['cloud-amqp-config']
    database_config_file_path = args['database-config']
    save_file_directory = args['save-path']
    # print(cloud_amqp_config_file_path)
    # print(database_config_file_path)
    # print(save_file_directory)
    return (get_amqp_url_parameters(cloud_amqp_config_file_path),
        get_database_settings(database_config_file_path),
        get_save_file_full_path(save_file_directory))