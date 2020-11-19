from datetime import datetime, timezone, timedelta,date
from pymongo import MongoClient
import argparse
import os
import yaml
import logging
import logging.config
from json import JSONEncoder

def remove_a_key(d, remove_key):
    if isinstance(d, dict):
        for key in list(d.keys()):
            if key == remove_key:
                del d[key]
            else:
                remove_a_key(d[key], remove_key)
    return d

def getColumnNamesFormatted(df):
    df.columns = df.columns.str.lower()
    df.columns = df.columns.str.strip()
    df.columns = map(lambda x: x.replace("-", "_").replace(" ", "_").replace("/","_"), df.columns)
    df = df.fillna("")
    return df

class DateTimeEncoder(JSONEncoder):
    # Override the default method
    def default(self, obj):
        if isinstance(obj, (date, datetime)):
            return obj.isoformat()
def datetime_handler(x):
    if isinstance(x, datetime):
        return x.isoformat()
    raise TypeError("Unknown type")

def getTotalStats(df):
    stats = df.tail(3)
    stats = stats[stats.columns[0:2]].transpose()
    stats = stats[stats.columns[0:3]].tail(2)
    header = stats.iloc[0]
    stats = stats[1:].rename(columns=header).reset_index(drop=True)
    stats = getColumnNamesFormatted(stats)
    return int(stats.loc[0, 'total_rows']), float(stats.loc[0, 'total_amount']), int(stats.loc[0, 'total_units'])

def getFormattedDate(row):
    month, day, year = row.split('/')
    utcdate = datetime(int(year), int(month), int(day), 0, 0, 0, 000, tzinfo=timezone.utc)
    utcdate = datetime.fromisoformat(str(utcdate))
    return utcdate

def getCurrentDate():
    current_date = datetime.now(timezone.utc)
    return current_date

def getUTCdate(year, month):
    utcdate = datetime(year, month, 1, 0, 0, 0, 000, tzinfo=timezone.utc)
    utcdate = datetime.fromisoformat(str(utcdate))
    return utcdate

def date_formatter(date_data):
    data = datetime.strptime(date_data, '%Y%m%d')
    return data.strftime('%d-%b-%Y')

def datetime_range(start=None, end=None):
    span = end - start
    for i in range(span.days + 1):
        yield (start + timedelta(days=i)).strftime('%Y%m%d')

def get_daterange(start_date, end_date):
    dates = list(
        datetime_range(start=datetime.strptime(start_date, "%Y%m%d"), end=datetime.strptime(end_date, "%Y%m%d")))
    return dates

def connectToMongo(db_server):
    client = MongoClient(db_server)
    return client

def writeToMongo(db_con, database, collection, record):
    db = db_con[database]
    collection = db[collection]
    collection.insert_one(record)

def get_config(config_file):
    """Loads configuration file.
    Parameters:
        config_file (str): path to file containing configuration
    Returns:
        config: dictionary containing values from parser YAML file
    """

    logger = logging.getLogger(__name__)
    with open(config_file, "rt") as f:
        try:
            config = yaml.safe_load(f.read())
            return config
        except yaml.YAMLError as e:
            logger.error("Load of config file %s failed", config_file)

def get_parser():
    """Gets parser object for this script"""

    parser = argparse.ArgumentParser()
    parser.add_argument("-r", "--report",
                        # dest="environemt",
                        help="specify report type itc_sales_report or itc_earnings_report",
                        # type=lambda x: is_valid_file(parser, x),
                        required=True)
    parser.add_argument("-c", "--config",
                        dest="config",
                        help="path to configuration file",
                        type=lambda x: is_valid_file(parser, x),
                        required=True)
    parser.add_argument("-e", "--env",
                        #dest="environemt",
                        help="specify environment. Staging or Production",
                        #type=lambda x: is_valid_file(parser, x),
                        required=True)

    return parser

def is_valid_file(parser, arg):
    """
    Checks if arg is valid file that exists on the local file system.
    Parameters:
        parser (ArgumentParser): ArgumentParser object
        arg (str): file path to be checked for existence
    Returns:
        arg (str): path to file if this file exists on the local file system
    """

    arg = os.path.abspath(arg)
    if not os.path.exists(arg):
        parser.error("The file %s does not exists." % arg)
    else:
        return arg