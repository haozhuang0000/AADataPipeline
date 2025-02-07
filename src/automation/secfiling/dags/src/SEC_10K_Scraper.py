"""
TASK: Download 10K or 10K/A reports
"""
import sys
import os
base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, base_path)

import time

import urllib3
from bs4 import BeautifulSoup
import re
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import json
import pandas as pd
import numpy as np
import json
import os
import argparse
import datetime

import common_methods
from logger import Log
from tqdm import tqdm
from joblib import Parallel, delayed, parallel_backend
import joblib
import multiprocessing

from selenium.webdriver.common.by import By
from common_methods import get_driver, create_id, check_data_nonexist, connect_db
from Insert_DB import preprocess_compstat, process_SECfiling_Compstat

from fake_useragent import UserAgent
from random_user_agent.user_agent import UserAgent
from random_user_agent.params import SoftwareName, OperatingSystem

def processing_compstats():

    try:
        if args.type == '10-Q':
            CompStat_Path = os.environ['COMPSTAT_QUARTER_PATH']
        else:
            CompStat_Path = os.environ['COMPSTAT_ANNUAL_PATH']
    except KeyError:
        logger.info('Fail to extract Compstat data, please acquired up-to-date info from WRDS')
        raise KeyError("The environment variable 'COMPSTAT_PATH' is not set, Please acquired up-to-date info from WRDS")
    list_CompStat = os.listdir(CompStat_Path)
    list_CompStat = [i for i in list_CompStat if i.endswith('csv')]
    if len(list_CompStat) > 1:
        raise KeyError("Please keep only one csv file in the COMPSTAT_PATH")
    else:
        logger.info('Start reading Compstat data...')
        selected_path = os.path.join(CompStat_Path, 'selected')
        if not os.path.exists(selected_path):
            os.makedirs(selected_path)
        try:
            df_compstat = pd.read_csv(os.path.join(selected_path, 'processed_compstat.csv'))
        except:
            df_compstat = pd.read_csv(os.path.join(CompStat_Path, list_CompStat[0]))
            df_compstat = preprocess_compstat(df_compstat)
            df_compstat.to_csv(os.path.join(selected_path, 'processed_compstat.csv'), index=False)
    return df_compstat


def get_user_agent():
    """
    Description: Use UserAgent to create random user agent
    Input: None
    Output: String - user agent
    """
    software_names = [SoftwareName.CHROME.value, SoftwareName.FIREFOX.value, SoftwareName.EDGE.value, SoftwareName.OPERA.value]
    operating_systems = [OperatingSystem.UNIX.value, OperatingSystem.WINDOWS.value, OperatingSystem.LINUX.value, OperatingSystem.MAC.value]
    user_agent_rotator = UserAgent(software_names=software_names, operating_systems=operating_systems, limit=1000)
    # Get list of user agents.
    user_agents = user_agent_rotator.get_user_agents()
    # Get Random User Agent String.
    user_agent = user_agent_rotator.get_random_user_agent()
    return user_agent

def retry_request(url, headers, max_retries=3, backoff_factor=0.3):
    session = requests.Session()
    retry = Retry(
        total=max_retries,
        read=max_retries,
        connect=max_retries,
        backoff_factor=backoff_factor,
        status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session.get(url, headers=headers)

def get_links(headers, cik):
    # Define base URL parts for constructing the SEC EDGAR search URL
    base_url_part1 = "http://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK="
    base_url_part2 = f"&type={args.type}&dateb=&owner=&start="
    base_url_part3 = "&count=100&output=xml"
    hrefs = []
    href_dict = {}
    ciknotype = []

    # Construct the complete URL for the initial page of search results
    base_url = base_url_part1 + str(cik) + base_url_part2 + "0" + base_url_part3
    if args.scrape_method == 'requests':
        html = retry_request(base_url, headers=headers)
        if html.status_code == 403:
            raise PermissionError("403 Forbidden Error: Access is denied. Please change scrape_method to `selenium`")
        bsObj = BeautifulSoup(html.text, "html.parser")
    elif args.scrape_method == 'selenium':
        driver.get(base_url)
        bsObj = BeautifulSoup(driver.page_source, "html.parser")


    # Find all 'filing' elements in the parsed HTML
    all_links = bsObj.find_all('filing')
    for links in all_links:
        # Extract the type of filing, href, and report date
        filing = links.find('type').text
        href = links.find('filinghref').text
        report_date = links.find('datefiled').text
        _id = create_id(cik, report_date, args.type)
        if check_data_nonexist('_id', _id, collection=collection):
            if filing == args.type and report_date > args.date:
                hrefs.append([href, filing, report_date])
    if all_links == []:
        all_links = bsObj.find_all('tr')
        for links in all_links[1:]:
            # Extract the type of filing, href, and report date
            filing = links.find_all('td')[0].text
            href = links.find_all('td')[1].a['href']
            report_date = links.find_all('td')[2].text
            _id = create_id(cik, report_date, args.type)
            if check_data_nonexist('_id', _id, collection=collection):
                if filing == args.type and report_date > args.date:
                    hrefs.append([href, filing, report_date])

    return hrefs


def extract_report_link(headers, url, cik):
    target_base_url = 'http://www.sec.gov'
    if args.scrape_method == 'requests':
        html = retry_request(url, headers=headers)
        bsObj = BeautifulSoup(html.text, 'html.parser')
    elif args.scrape_method == 'selenium':
        driver.get(url)
        bsObj = BeautifulSoup(driver.page_source, 'html.parser')
    xbrl_file = bsObj.findAll('tr')[1:]
    i = 0
    target_url = None
    target_txt_url = None
    for item in xbrl_file:
        try:
            # Check if the fourth column (index 3) of the current row matches the target file type
            if item.findAll('td')[3].get_text() == args.type:
                # Check if the URL in the third column (index 2) ends with '.htm'
                  if str(target_base_url + item.findAll('td')[2].find('a')['href']).endswith('.htm'):
                    # Construct the target URL by combining the base URL and the URL from the third column
                    target_url = target_base_url + item.findAll('td')[2].find('a')['href']
                    # print("Target URL found!")
                    print("Target URL is:", target_url)

            # Check if the second column (index 1) of the current row contains the text 'Complete submission text file'
            if item.findAll('td')[1].get_text() == "Complete submission text file":
                # Construct the target text URL by combining the base URL and the URL from the third column
                target_txt_url = target_base_url + item.findAll('td')[2].find('a')['href']
                print("Target Text URL is:", target_txt_url)
                break
        except Exception as e:
            # logger.info(f'Fail to extract links for {cik}')
            pass
        i += 1
    return target_url, target_txt_url


def get_article_contents(headers, url, url_txt, dir_path, cik, reporting_date, df_compstat_cik):
    """
    PLease be aware that SEC Provide two type of file,
    a. TXT
        We only extract the header section from TXT file because this TXT file is with messy format which it is very hard to parse
    b. HTML
        extract full text from it
    :param url: HTML URL
    :param url_txt: FULL TEXT URL
    """
    type_ = args.type
    _id = create_id(cik, reporting_date, type_)
    ## Getting Header from url_txt
    if url_txt is not None:
        ## Get text from .txt
        if args.scrape_method == 'requests':
            html = retry_request(url_txt, headers=headers)
            # raw_txt = html.text
            text_original = html.text
        elif args.scrape_method == 'selenium':
            driver.get(url_txt)
            time.sleep(0.5)
            text_original = driver.find_element(By.XPATH, "/html/body").text

        # Extract the SEC header section from txt file
        try:
            regex = re.compile(r'</SEC-HEADER>')
            matches = regex.finditer(text_original)
            for match in matches:
                end = match.end()
            text_header = text_original[:end]
        except:
            try:
                regex = re.compile(r'</IMS-HEADER>')
                matches = regex.finditer(text_original)
                for match in matches:
                    end = match.end()
                text_header = text_original[:end]
            except:
                logger.info(f'{cik}-{reporting_date} does not have header!!')
                text_header = ''
    ## Getting Full Text from url
    if url is not None:

        url = url.replace('/ix?doc=', '')
        if args.scrape_method == 'requests':
            html_ = retry_request(url, headers=headers)
            text_original = html_.text
            bsObj = BeautifulSoup(html_.text, 'html.parser')
        elif args.scrape_method == 'selenium':
            driver.get(url)
            time.sleep(0.5)
            text_original = driver.page_source
            bsObj = BeautifulSoup(driver.page_source, 'html.parser')

        # if not os.path.exists(dir_path):
        #     os.mkdir(dir_path)
        text = bsObj.find('body').get_text(separator="\n")

        # Clean up the text by removing excessive whitespace and non-breaking spaces
        text = re.sub(r'\s{10,}', '\n', text)
        text = re.sub(r'\s{10,}', '\n', text)
        # start = 0
        # Find index that start with Form 10k, so that useless contents are ignored
        regex = re.compile(r'Form\s*\n*{0}'.format(args.type), re.IGNORECASE)
        matches = regex.finditer(text.lower())
        for match in matches:
            start = match.start()
            break
        text = text[start:]

        text = text.replace('\xa0', '')
        text = re.sub(r'(\n){2,}', '\n', text)
        text = re.sub(r'(\xa0\n){3,}', '\n', text)
        ## Text
        text_final = text_header + "\n\n" + text + "\n\n"
        df_txt = pd.DataFrame({
            '_id': [_id],
            'cik': [cik],
            'date': [reporting_date],
            'type': [type_],
            'content': [text_final]
        })
        data_txt = process_SECfiling_Compstat(df_txt, df_compstat_cik, args.type)
        data_txt.filing_date = data_txt.filing_date.astype(str)
        data_txt.datadate = data_txt.datadate.astype(str).replace('NaT', '')
        data_txt = data_txt.fillna('')
        data_txt = data_txt[['_id', 'cik', 'gvkey', 'tic', 'conm', 'filing_date', 'fyear', 'fmonth', 'fyear_fqtr', 'type', 'content']]
        common_methods.insert_db_one(data_txt, col_name=col_name, dbs_name='AIDF_NLP_Capstone_Temp')

        ## HTML
        data_html = {
            '_id': _id,
            'cik': cik,
            'date': reporting_date,
            'type': type_,
            'content': text_original
        }
        common_methods.insert_db_one(data_html, col_name=col_name_HTML, dbs_name="AIDF_NLP_Capstone_Temp")
        logger.info(f"Successfully download ticker: {cik} Date: {reporting_date}")

    ## Getting Full Text from url_txt if this company in this year does not have any HTML published
    if url is None and url_txt is not None:
        ## Get text from .txt
        if args.scrape_method == 'requests':
            html = retry_request(url_txt, headers=headers)
            text_original = html.text
        elif args.scrape_method == 'selenium':
            driver.get(url_txt)
            time.sleep(0.5)
            text_original = driver.find_element(By.XPATH, "/html/body").text

        text_final = text_original + "\n\n"
        df_txt = pd.DataFrame({
            '_id': [_id],
            'cik': [cik],
            'date': [reporting_date],
            'type': [type_],
            'content': [text_final]
        })

        data_txt = process_SECfiling_Compstat(df_txt, df_compstat_cik, args.type)
        data_txt.filing_date = data_txt.filing_date.astype(str)
        data_txt.datadate = data_txt.datadate.astype(str).replace('NaT', '')
        data_txt = data_txt.fillna('')
        data_txt = data_txt[['_id', 'cik', 'gvkey', 'tic', 'conm', 'filing_date', 'fyear', 'fmonth', 'fyear_fqtr', 'type', 'content']]
        common_methods.insert_db(data_txt, col_name=col_name, dbs_name='AIDF_NLP_Capstone_Temp')

    return [cik, reporting_date, url, url_txt]

def run(cik, base_path, df_compstat):

    # current_process = multiprocessing.current_process()
    # processor = current_process.name
    df_compstat_cik = df_compstat[df_compstat.cik.apply(lambda x: x == float(cik))]
    list_summary = []
    list_fail = []
    # if str(cik) in cik_downloaded_list or cik in cik_no10k_list:
    # if str(cik) in cik_downloaded_list:
    #     print(f"Already downloaded: {cik}")
    #     if args.scrape_method == 'selenium':
    #         return None, None, None
    #     else:
    #         return None

    ## Get links
    url_list = get_links(headers, cik)
    # list_no10k.append(no10k)
    for url_filing_date in url_list:
        url = url_filing_date[0]
        filing = url_filing_date[1]
        date = url_filing_date[2]
        # try:
            ## Extract HTML URL & TXT URL from each links
        file_link, txt_link = extract_report_link(headers, url, cik)
        if file_link == None and txt_link == None:
            logger.info(f'Unable to find Links for {cik}-{date}')
            continue

        dir_path = base_path + "/" + str(cik)
        try:
            ## Extract content from URLs
            out_summary = get_article_contents(headers, file_link, txt_link, dir_path, cik, date, df_compstat_cik)
            list_summary.append(out_summary)
        except Exception as e:
            logger.info(f'fail to extract content for {cik}-{date}')
            _id = create_id(cik, date, args.type)
            fail_dict = {
                '_id': _id,
                'cik': cik,
                'date': date,
                'url': url,
                'type': args.type,
                'reason': str(e)
            }
            common_methods.insert_db_one(fail_dict, col_name='Airflow_Test_Level1_10K_ScrapFail', dbs_name='AIDF_NLP_Capstone_Temp')
            list_fail.append([cik, date, url, e])
            pass
    return list_summary, list_fail

def storing_summary(list_summary_output, list_fail_output):

    summary_df = []
    for i in list_summary_output:
        if i is not None:
            # df_temp = pd.DataFrame(i, columns=["cik", "reporting_date", "text_path", "html_path", "full_text_path", "url", "url_txt"])
            try:
                df_temp = pd.DataFrame(
                    {'cik': [i[0]], 'reporting_date': [i[1]], 'url': [i[2]], 'url_txt': [i[3]]})
            except:
                df_temp = pd.DataFrame(i, columns=[
                    ['cik', 'reporting_date', 'url', 'url_txt']])
            summary_df.append(df_temp)
    try:
        df_summary = pd.concat(summary_df)
        df_summary.to_csv(os.path.join(summary_path, f"{args.type}_summary_{start}_{end}.csv".replace('/', '')),
                          index=False)
    except:
        print("Everything in this batch downloaded!!")
        pass

    ## this df store those ciks that fail downloaded
    fail_df = []
    for i in list_fail_output:
        if i is not None:
            try:
                df_temp = pd.DataFrame({'cik': [i[0]], 'reporting_date': [i[1]], 'url': [i[2]], 'reason': [i[3]]})
            except:
                df_temp = pd.DataFrame(i, columns=['cik', 'reporting_date', 'url', 'reason'])
            fail_df.append(df_temp)
    try:
        df_fail = pd.concat(fail_df)
        df_fail.to_csv(os.path.join(summary_path, f"{args.type}_fail_summary_{start}_{end}.csv".replace('/', '')),
                       index=False)
    except:
        logger.info(f"No failed {args.type} found in the batch from {start} to {end}.")
        pass

    # ## dne stands for 'does not exist', this df store those ciks that does not have any specific type
    # type_dne_df = []
    # no10k_df = []
    # for i in list_no10k_output:
    #     # df_temp = pd.DataFrame(i, columns=["cik"])
    #     if i is not None:
    #         try:
    #             df_temp = pd.DataFrame({'cik': [i[0]]})
    #         except:
    #             df_temp = pd.DataFrame(i, columns=['cik'])
    #         type_dne_df.append(df_temp)
    # try:
    #     df_no10k = pd.concat(type_dne_df)
    #     df_no10k.to_csv(os.path.join(summary_path, f"{args.type}_dne_summary_{start}_{end}.csv".replace('/', '')),
    #                     index=False)
    # except:
    #     logger.info(f"No missing {args.type} found in the batch from {start} to {end}.")
    #     pass

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--date',
                        type=str,
                        required=False,
                        default="1900-01-01",
                        help="Select storage date")
    parser.add_argument('--type',
                        type=str,
                        required=False,
                        default="10-K",
                        help="Select file type")
    parser.add_argument('--scrape_method',
                        type=str,
                        required=True,
                        help="What method to scrape (requests or selenium), please note if you get 403 using request please use selenium")
    args, unknown = parser.parse_known_args()
    logger = Log(f'Start SEC {args.type} Crawler'.replace('/', '')).getlog()
    ua = UserAgent()
    headers = {
        'User-Agent': "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36"}
    headers = {'User-Agent': get_user_agent()}

    col_name = 'Airflow_Test_Level1_10K_AutoTest'
    col_name_HTML = 'Airflow_Test_Level1_10K_HTML'

    DB = connect_db('AIDF_NLP_Capstone_Temp')
    collection = DB[col_name]

    df_compstat = processing_compstats()
    # headers = {'User-Agent': ua.random}
    if args.scrape_method == 'selenium':
        driver = get_driver(True)
    with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'comp_cik_full.json'), "r") as json_file:
        TickerFile = json.load(json_file)
    cik_ticker = {}
    cik_str = [i for i in TickerFile.keys()]

    base_path = f"./Downloaded_Filings_{args.type.replace('/', '')}/raw"
    summary_path = os.path.join(f"./Downloaded_Filings_{args.type.replace('/', '')}/summary_scraper")
    if not os.path.exists(base_path):
        os.makedirs(base_path)
    if not os.path.exists(summary_path):
        os.makedirs(summary_path)
    root_file_list = os.listdir(summary_path)
    max_number = 0

    ## Search downloaded ticker
    for file in root_file_list:
        if '{0}_summary'.format(args.type.replace('/', '')) in file:
            matches = re.findall(r'{0}_summary_(\d+)_(\d+)\.csv'.format(args.type.replace('/', '')), file)
            if matches:
                # Extract the second number and update max_number if it's higher
                _, num2 = matches[0]
                max_number = max(max_number, int(num2))


    ## Scrape it with request
    for start in range(max_number, len(cik_str), 50):
        end = start + 50

        # Initialize lists to collect all results
        list_summary_output = []
        list_fail_output = []

        ## Scrape it with request
        if args.scrape_method == 'requests':
            with parallel_backend('threading', n_jobs=10):
                results = Parallel(n_jobs=10)(delayed(run)(cik, base_path, df_compstat) for cik in tqdm(cik_str[start:end]))
            # Unpack results into separate lists
            for result in results:
                if result is not None and isinstance(result, tuple) and len(result) == 3:
                    summary, fail = result
                    list_summary_output.extend(summary)
                    list_fail_output.extend(fail)

        elif args.scrape_method == 'selenium':
            for cik in tqdm(cik_str[start:end]):
                if check_data_nonexist('cik', int(cik), collection=collection):
                    summary, fail = run(cik, base_path, df_compstat)

                    list_summary_output.append(summary)
                    list_fail_output.append(fail)
            # with parallel_backend('threading', n_jobs=3):
            #     results = Parallel(n_jobs=3)(delayed(run)(cik, base_path) for cik in tqdm(cik_str[start:end]))
            # # Unpack results into separate lists
            # for result in results:
            #     if result is not None and isinstance(result, tuple) and len(result) == 3:
            #         summary, fail = result
            #         list_summary_output.extend(summary)
            #         list_fail_output.extend(fail)

        storing_summary(list_summary_output, list_fail_output)