from seleniumbase import Driver
import argparse
import time
import urllib
# from selenium import webdriver
from datetime import datetime
import os
import pandas as pd
from pymongo import MongoClient
from dotenv import load_dotenv, find_dotenv
import uuid
import bson
from bson.binary import Binary, UuidRepresentation
import chromedriver_autoinstaller
from selenium.webdriver.chrome.options import Options
from selenium import webdriver

_ = load_dotenv(find_dotenv())
def get_driver(isHeadless=False):
    # chrome_options = Options()
    # chrome_path = "C:\\Program Files\\Google\\Chrome\\Application\\chrome"
    # chrome_options.binary_location = chrome_path
    # driver = Driver(uc=True, headless2=isHeadless, binary_location=chrome_path) # Pass the chrome_options to the Driver

    options = Options()
    if isHeadless:
        options.add_argument("--headless")  # Run Chrome in headless mode
    # remote_webdriver = 'remote_chromedriver'
    remote_webdriver = "http://remote_chromedriver:4444/wd/hub"  # Change if needed
    driver = webdriver.Remote(command_executor=remote_webdriver, options=options)
    return driver


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