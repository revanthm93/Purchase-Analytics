from reporter import *
import pandas as pd
from utilities  import *
import json
import sys
import logging
import pymongo

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def getSalesReport(rep, appstore_config, db_server, date):
    try:
        df = getColumnNamesFormatted(rep.download_sales_report(vendor= appstore_config['vendor'], report_type= appstore_config['report_type'], \
                                                               report_version= appstore_config['report_version'], date_type= appstore_config['date_type'], \
                                                               date= date, report_subtype= appstore_config['report_subtype']))
        logger.info("Sales report available for given date %s ", date)
        df['begin_date'] =  df['begin_date'].apply(lambda row : getFormattedDate(row))
        df['end_date'] = df['end_date'].apply(lambda row: getFormattedDate(row))
        db_con = connectToMongo(db_server)
        for i, x in df.groupby('country_code'):
            records = x.to_dict(orient='records')
            _id = x[['begin_date','end_date','country_code']].copy().drop_duplicates()
            _id = _id.to_dict(orient='records')[0]
            jsondata = {}
            jsondata['_id'] = _id
            jsondata['reports'] = records
            jsondata['daily'] = True
            jsondata['created_at'] = getCurrentDate()
            #writeToMongo(db_con = db_con, database = appstore_config['database'], collection= appstore_config['collection'], record= jsondata)
            logger.info("Sales Report successfully written to mongo database:%s -> collection:%s", appstore_config['database'], appstore_config['collection'])
    except requests.exceptions.HTTPError as e:
        logger.error("Sales report not available for date %s. %s", date, e, exc_info=True)
    except pymongo.errors.DuplicateKeyError as e:
        logger.error("Durplicate key error %s", e)
    except Exception as e:
        logger.error("Error in transforming/writing reports. %s ", e, exc_info=True)

def getEarningsReport(rep, appstore_config, db_server, region):
    try:
        df = rep.download_financial_report(vendor = appstore_config['vendor'], region_code= region, report_type= appstore_config['report_type'], \
                                           fiscal_year = appstore_config['fiscal_year'] , fiscal_period = appstore_config['fiscal_period'])
        logger.info("Earnings report available for region %s for the date specified %s year and %sth period." , region, appstore_config['fiscal_year'], appstore_config['fiscal_period'])
        raw_df = getColumnNamesFormatted(df)
        df = raw_df.iloc[:-3].copy()
        df['start_date'] = df['start_date'].apply(lambda row: getFormattedDate(row))
        df['end_date'] = df['end_date'].apply(lambda row: getFormattedDate(row))
        rows = df.to_dict(orient='records')
        jsondata = {}
        _id = {}
        _id['date'] = getUTCdate(int(appstore_config['fiscal_year']), int(appstore_config['fiscal_period']))
        _id['region'] = region
        jsondata['_id'] = _id
        jsondata['created_at'] = getCurrentDate()
        jsondata['year'] = int(appstore_config['fiscal_year'])
        jsondata['month'] = int(appstore_config['fiscal_period'])
        jsondata['rows'] = rows
        jsondata['total_rows'] = getTotalStats(raw_df)[0]
        jsondata['total_amount'] = getTotalStats(raw_df)[1]
        jsondata['total_units'] = getTotalStats(raw_df)[2]
        db_con = connectToMongo(db_server)
        writeToMongo(db_con=db_con, database=appstore_config['database'], collection=appstore_config['collection'], record=jsondata)
        logger.info("Report successfully written to mongo database:%s -> collection:%s", appstore_config['database'], appstore_config['collection'])
    except requests.exceptions.HTTPError as e:
        logger.error("Earnings report not available for region %s for the date specified %s year and %sth period.", region, appstore_config['fiscal_year'], appstore_config["fiscal_period"])
    except pymongo.errors.DuplicateKeyError as e:
        logger.error("Durplicate key error %s", e, exc_info=True)
    except Exception as e:
        logger.error("Error in transforming/writing reports. %s ", e, exc_info=True)

def main(argv):
    args = get_parser().parse_args()
    config_path = args.config
    env = args.env
    report = args.report
    config = get_config(config_path)
    appstore_config = config[report]
    db_server = config[env]['db_server']
    appstore_token = config['appstore_access_token']
    if report == 'itc_sales_report':
        rep = Reporter(access_token= appstore_token)
        if appstore_config['bulkmode'] == 1:
            dates = get_daterange(appstore_config['bulk_startdate'], appstore_config['bulk_enddate'])
            for date in dates:
                getSalesReport(rep, appstore_config, db_server, date)
            logger.info("Sales Report for bulk date range successfully written to mongo database:%s -> collection:%s", appstore_config['database'], appstore_config['collection'])
        else:
            getSalesReport(rep, appstore_config, db_server, appstore_config['date'])
    if report == 'itc_earnings_report':
        rep = Reporter(access_token= appstore_token)
        for region in appstore_config['region_codes']:
            getEarningsReport(rep, appstore_config, db_server, region)

if __name__ == "__main__":
        main(sys.argv)