from datetime import datetime
import os
import pandas as pd
from pymongo import MongoClient
from dotenv import load_dotenv, find_dotenv
import uuid
import bson
from bson.binary import Binary, UuidRepresentation

_ = load_dotenv(find_dotenv())

def connect_db(dbs='AIDF_NLP_Capstone'):

    DB_URL = os.environ['LOCAL_URL']
    client = MongoClient(DB_URL)
    db = client[dbs]
    return db

def insert_db(data, col_name, dbs_name='AIDF_NLP_Capstone'):

    if isinstance(data, pd.DataFrame):
        DB = connect_db(dbs=dbs_name)
        collection = DB[col_name]
        collection.insert_many(data.to_dict('records'))
    if isinstance(data, list):
        DB = connect_db(dbs=dbs_name)
        collection = DB[col_name]
        collection.insert_many(data)

def insert_db_one(data, col_name, dbs_name='AIDF_NLP_Capstone'):
    DB = connect_db(dbs_name)
    collection = DB[col_name]
    try:
        if isinstance(data, dict):
            collection.insert_one(data)
        elif isinstance(data, pd.DataFrame):
            collection.insert_one(data.to_dict('records')[0])
    except Exception as e:
        print(e)
        pass


def create_id(cik, date, type):

    _id = uuid.uuid3(uuid.NAMESPACE_DNS, str(cik) + date + type)
    _id = bson.Binary.from_uuid(_id, uuid_representation=UuidRepresentation.PYTHON_LEGACY)
    return _id

def determine_quarter(date_str):

    date = datetime.strptime(date_str, '%Y-%m-%d')
    month = date.month

    if (month == 1) or (month == 2) or (month == 3):
        return 1
    elif (month == 4) or (month == 5) or (month == 6):
        return 2
    elif (month == 7) or (month == 8) or (month == 9):
        return 3
    else:
        return 4  # Covers October, November, December


def check_data_nonexist(key, value, collection, dbs_name='AIDF_NLP_Capstone'):

    find_cursor = collection.find({key: value})
    if [i for i in find_cursor] == []:
        return True
    elif [i for i in find_cursor] != []:
        return False

def preprocess_compstat(df_compstat):
    df_compstat = df_compstat.dropna(subset=['cik'])
    try:
        df_compstat = df_compstat[['gvkey', 'datadate', 'fyearq', 'fyr', 'fqtr', 'cik', 'tic', 'conm']]
    except:
        df_compstat = df_compstat[['gvkey', 'datadate', 'fyear', 'fyr', 'cik', 'tic', 'conm']]
    df_compstat = df_compstat.drop_duplicates()
    return df_compstat

def process_SECfiling_Compstat(df_10k, df_compstat, sec_type):

    if sec_type == '10-K' or sec_type == '10-K/A':
        def _compute_fyear(x):
            filing_date = x.filing_date
            data_date = x.datadate
            fyear = x.fyear
            if not pd.isnull(fyear):
                if filing_date > data_date:
                    return fyear
                else:
                    return fyear - 1
            else:
                return None
        df_10k['date'] = pd.to_datetime(df_10k.date)
        df_10k['year_10k'] = df_10k.date.apply(lambda x: x.year)
        df_10k['year_10k'] = df_10k.year_10k.apply(lambda x: int(x))
        df_10k['cik'] = df_10k.cik.apply(lambda x: float(x))
        df_10k = df_10k.rename(columns={'date': 'filing_date'})

        df_compstat.loc[:, 'datadate'] = pd.to_datetime(df_compstat['datadate'])
        df_compstat = df_compstat.dropna(subset=['fyear', 'fyr'])
        df_compstat['fyear'] = df_compstat.fyear.apply(lambda x: int(x))
        df_compstat['year_10k'] = df_compstat.fyear.apply(lambda x: int(x))
        df_compstat['fyr'] = df_compstat.fyr.apply(lambda x: int(x))
        df_compstat = df_compstat.rename(columns={'fyr': 'fmonth'})


        df_out = df_10k.merge(df_compstat, on=['cik', 'year_10k'], how='left')
        # df_out = df_out.rename(columns={'fyear': 'year_10k'})
        df_out['fyear'] = df_out.apply(lambda x: _compute_fyear(x), axis=1)
        df_out['fyear_fqtr'] = ''
        df_out = df_out.drop(columns=['year_10k'])

    elif sec_type == '10-Q':

        def _compute_fyear_fqtr(x):

            filing_date = x.filing_date
            data_date = x.datadate
            fyear = x.fyearq
            if not pd.isnull(fyear):
                # if filing_date > data_date:
                fqtr = x.fqtr
                if fyear and fqtr:
                    if fqtr > 1:
                        fqtr_out = fqtr - 1
                        return str(fyear) + "Q" + str(fqtr_out)
                    else:
                        fyear_out = fyear - 1
                        fqtr_out = 4
                        return str(fyear_out) + "Q" + str(fqtr_out)
                else:
                    return None
                # else:
                #     raise KeyError(f'Can not determine the fiscal quarter, please take a look for this case!! cik: {x.cik} - filing_date: {x.filing_date}')
            else:
                return None

        def _split_fyaer_fqtr(x):
            if x is not None:
                return x.split('Q')[0]
            else:
                return None
        df_10k['date'] = pd.to_datetime(df_10k.date)
        df_10k['year_10k'] = df_10k.date.apply(lambda x: x.year)
        df_10k['year_10k'] = df_10k.year_10k.apply(lambda x: int(x))
        df_10k['cik'] = df_10k.cik.apply(lambda x: float(x))
        df_10k['fqtr'] = df_10k.date.apply(lambda x: determine_quarter(str(x.date())))
        df_10k = df_10k.rename(columns={'date': 'filing_date'})

        df_compstat.loc[:, 'datadate'] = pd.to_datetime(df_compstat['datadate'])
        df_compstat = df_compstat.dropna(subset=['fyearq', 'fyr'])
        df_compstat['fyearq'] = df_compstat.fyearq.apply(lambda x: int(x))
        df_compstat['year_10k'] = df_compstat.fyearq.apply(lambda x: int(x))
        df_compstat['fyr'] = df_compstat.fyr.apply(lambda x: int(x))
        df_compstat = df_compstat.rename(columns={'fyr': 'fmonth'})

        df_out = df_10k.merge(df_compstat, on=['cik', 'year_10k', 'fqtr'], how='left')
        # df_out = df_out.rename(columns={'fyear': 'year_10k'})
        df_out['fyear_fqtr'] = df_out.apply(lambda x: _compute_fyear_fqtr(x), axis=1)
        df_out['fyear'] = df_out.fyear_fqtr.apply(lambda x: _split_fyaer_fqtr(x))
        df_out = df_out.drop(columns=['year_10k', 'fqtr'])

    return df_out