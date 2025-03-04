import feedparser
from bs4 import BeautifulSoup
import requests
import os
import pandas as pd
from logger import Log
import re
import argparse
from utils import *
import warnings
warnings.simplefilter(action='ignore', category=pd.errors.SettingWithCopyWarning)

def processing_compstats():
    code_path = os.getcwd()
    try:
        if args.type == '10-Q':
            CompStat_Path = os.environ['COMPSTAT_QUARTER_PATH']
        else:
            CompStat_Path = os.environ['COMPSTAT_ANNUAL_PATH']
        CompStat_Path = os.path.join(code_path, CompStat_Path)
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

def parse_sec_edgar_rss(cik):
    """Fetches and parses the SEC EDGAR RSS feed for a given CIK."""
    rss_url = f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={cik}&type=&dateb=&owner=exclude&start=0&count=100&output=atom"
    response = requests.get(rss_url, headers=headers)

    if response.status_code == 403:
        print("Access blocked. Ensure your User-Agent string is set correctly.")
        return []

    feed = feedparser.parse(response.content)

    if 'entries' not in feed:
        print("No entries found in the RSS feed.")
        return []

    filings = []
    for entry in feed.entries:
        filing = {
            "cik": cik,
            "filing_type": entry.get("filing-type", "No Title"),
            "filing_href": entry.get("filing-href", "No Link"),
            "filing_date": entry.get("filing-date", "No Date")
        }
        filings.append(filing)

    return filings


def get_filing_links(filings: list, target_filing: list):
    filing_list = [filing for filing in filings if filing['filing_type'] in target_filing]
    href_list = []
    for filing in filing_list:

        if filing["filing_date"] <= limit_date:
            continue

        html = requests.get(filing['filing_href'], headers=headers)
        bsObj = BeautifulSoup(html.text, "html.parser")
        xbrl_file = bsObj.find_all('tr')[1:]
        target_url = None
        for item in xbrl_file:
            try:
                # Check if the fourth column (index 3) of the current row matches the target file type
                if item.find_all('td')[3].get_text() in target_filing:
                    # Check if the URL in the third column (index 2) ends with '.htm'
                    if str(target_base_url + item.find_all('td')[2].find('a')['href']).endswith('.htm'):
                        # Construct the target URL by combining the base URL and the URL from the third column
                        target_url = target_base_url + item.find_all('td')[2].find('a')['href']
                        # print("Target URL found!")
                        print("Target URL is:", target_url)

                if item.find_all('td')[1].get_text() == "Complete submission text file":
                    # Construct the target text URL by combining the base URL and the URL from the third column
                    target_txt_url = target_base_url + item.find_all('td')[2].find('a')['href']
                    print("Target Text URL is:", target_txt_url)
                    break

            except Exception as e:
                # logger.info(f'Fail to extract links for {cik}')
                pass
        href_list.append({'url_html': target_url,
                          'url_txt': target_txt_url,
                          'cik': filing['cik'],
                          "filing_date": filing["filing_date"],
                          "filing_type": filing["filing_type"]}
                         )
    return href_list

def get_article_contents(url, url_txt, cik, reporting_date, df_compstat_cik):
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
        html = requests.get(url_txt, headers=headers)
        # raw_txt = html.text
        text_original = html.text

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
        html_ = requests.get(url, headers=headers)
        text_original = html_.text
        bsObj = BeautifulSoup(html_.text, 'html.parser')

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
        insert_db_one(data_txt, col_name=col_name, dbs_name=db_name)

        ## HTML
        data_html = {
            '_id': _id,
            'cik': cik,
            'date': reporting_date,
            'type': type_,
            'content': text_original
        }
        insert_db_one(data_html, col_name=col_name_HTML, dbs_name=db_name_HTML)
        logger.info(f"Successfully download ticker: {cik} Date: {reporting_date}")

    # ## Getting Full Text from url_txt if this company in this year does not have any HTML published
    # if url is None and url_txt is not None:
    #     ## Get text from .txt
    #     html = requests.get(url_txt, headers=headers)
    #     text_original = html.text
    #
    #     text_final = text_original + "\n\n"
    #     df_txt = pd.DataFrame({
    #         '_id': [_id],
    #         'cik': [cik],
    #         'date': [reporting_date],
    #         'type': [type_],
    #         'content': [text_final]
    #     })
    #
    #     data_txt = process_SECfiling_Compstat(df_txt, df_compstat_cik, args.type)
    #     data_txt.filing_date = data_txt.filing_date.astype(str)
    #     data_txt.datadate = data_txt.datadate.astype(str).replace('NaT', '')
    #     data_txt = data_txt.fillna('')
    #     data_txt = data_txt[['_id', 'cik', 'gvkey', 'tic', 'conm', 'filing_date', 'fyear', 'fmonth', 'fyear_fqtr', 'type', 'content']]
    #     insert_db(data_txt, col_name=col_name)

    return [cik, reporting_date, url, url_txt]

def main(cik, df_compstat):

    filings = parse_sec_edgar_rss(cik)
    href_list = get_filing_links(filings, target_filings)
    for href in href_list:
        url = href['url_html']
        url_txt = href['url_txt']
        cik = href['cik']
        filing_date = href['filing_date']
        get_article_contents(url, url_txt, cik, filing_date, df_compstat)



if __name__ == "__main__":
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
    args, unknown = parser.parse_known_args()

    logger = Log(f'Start SEC {args.type} Crawler'.replace('/', '')).getlog()
    headers = {
        "User-Agent": "YourCompanyName ContactEmail@example.com",
        "Accept": "application/xml"
    }
    target_base_url = 'http://www.sec.gov'
    target_filings = ['10-K']

    limit_date = "2022-01-01"
    cik = "0000320193"  # Apple Inc. CIK as an example

    col_name = 'Level1_10K_rss_feed'
    db_name = "AIDF_AlternativeData"

    col_name_HTML = 'Level1_10K_rss_feed_html'
    db_name_HTML = "AIDF_AlternativeData"

    df_compstat = processing_compstats()
    main(cik, df_compstat)