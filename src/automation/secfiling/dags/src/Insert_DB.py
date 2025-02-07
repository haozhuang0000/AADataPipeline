"""
Task:
1. Insert Original 10K Text into `Level1_10K`
2. Insert Extracted Items into `Level2_10K`
3. Insert Fstats into `Level3_10K`
"""
import common_methods
from common_methods import connect_db, insert_db, create_id, determine_quarter
import os
import pandas as pd
from logger import Log
from tqdm import tqdm
import json

# def extract_ticker(x):
#
#     if x.Cik in TickerFile:
#         return TickerFile[str(x.Cik)]
#     elif x.Cik in TickerFile_dict:
#         return TickerFile_dict[x.Cik]
#     else:
#         return None

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

def insert_original_text(df_compstat):

    # Define the main path where the files are stored
    main_path = os.environ['TEXT_PATH']
    list_cik = os.listdir(main_path)
    list_text = []
    # Loop through each CIK directory
    for cik in tqdm(list_cik):
        ticker_path = os.path.join(main_path, cik)
        
        # List all text files for the current CIK
        list_comp_year_txt = os.listdir(ticker_path)
        for comp_year_txt in list_comp_year_txt:
            if comp_year_txt.endswith('.txt'):
                if 'header' not in str(comp_year_txt):
                    try:
                        comp_year_path = os.path.join(main_path, cik, comp_year_txt)
                        with open(comp_year_path, 'r', encoding='utf-8') as f:
                            text = f.read()
                            f.close()
                        # If the text length is less than or equal to 10000 characters, then ignore
                        if len(text) <= 10000:
                            continue
                        text_list = text.split('\n')

                        # Remove lines containing 'END PRIVACY-ENHANCED MESSAGE' from the text
                        text = [i for i in text_list if 'END PRIVACY-ENHANCED MESSAGE' not in i]
                        text = '\n'.join(text)
                        filename = comp_year_txt.replace('.txt', '')
                        # Extract date and type from the filename
                        date, type = filename.split('_')[1], filename.split('_')[2]

                        # Create a unique ID using the CIK and date
                        _id = create_id(cik, date, type)
                        list_text.append([_id, cik, date, type, text])
                    except Exception as e:
                        print(e)
                        pass
    df_out = pd.DataFrame(list_text, columns=['_id', 'cik', 'date', 'type', 'content'])
    df_out = process_SECfiling_Compstat(df_out, df_compstat)
    df_out.filing_date = df_out.filing_date.astype(str)
    df_out.datadate = df_out.datadate.astype(str).replace('NaT', '')
    df_out = df_out.fillna('')
    df_out = df_out[['_id', 'cik', 'gvkey', 'tic', 'conm', 'filing_date', 'fyear', 'fmonth', 'type', 'content']]
    for i, df_temp in tqdm(df_out.iterrows()):
        common_methods.insert_db_one(df_temp.to_dict(), 'Level1_10K')

    # insert_db(df_out, 'Level1_10K')

def insert_items(df_compstat):

    def _search_more_sections(path, filename):
        final_text = ''
        list_extracted_items = os.listdir(path)
        count = 0
        for item in list_extracted_items:
            if filename.replace('.txt', '') in item:
                more_section_path = os.path.join(path, item)
                if os.path.exists(more_section_path):
                    with open(more_section_path, 'r', encoding='utf-8') as f:
                        item_more_section_text = f.read()
                        final_text += item_more_section_text
                        count += 1
                        f.close()
        return final_text

    ITEM_PATH = os.environ['OUTPUT_ITEM_PATH']
    SUMMARY_PATH = os.environ['SUMMARY_PATH']
    list_summary_path = os.listdir(SUMMARY_PATH)
    df = None
    for i in list_summary_path:
        if '10-K_items_summary_final' in i and i.endswith('.csv'):
            df = pd.read_csv(os.path.join(SUMMARY_PATH, i))
            break
    if df is None:
        raise FileNotFoundError("The file '10-K_items_summary_final.csv' does not exist in the provided path list.")

    list_items = []
    for i, row in tqdm(df.iterrows()):
        try:
            # Process Item1 text if filename exists
            if not pd.isna(row['filename_item1']):
                _item1_path = os.path.join(ITEM_PATH, str(row['cik']), 'item1business', 'item1_final', row['filename_item1'])
                if os.path.exists(_item1_path):
                    with open(_item1_path, 'r', encoding='utf-8') as f:
                        item1_text = f.read()
                        f.close()
                else:
                    item1_text = None
                ###################### Item 1: More Section ######################
                if item1_text == None:
                    item1_text = _search_more_sections(os.path.join(ITEM_PATH, str(row['cik']), 'item1business', 'item1_final'), row['filename_item1'])
                # _item1_path = os.path.join(ITEM_PATH, 'item1business', 'item1_final', row['filename_item1'].replace('.txt', '') + '_more_section'+ '.txt')
                # if os.path.exists(_item1_path):
                #     with open(_item1_path, 'r', encoding='utf-8') as f:
                #         item1_text_more_section = f.read()
                #         if item1_text_ori is None:
                #             item1_text = item1_text_more_section
                #         else:
                #             item1_text = item1_text_ori + '\n' + item1_text_more_section
                #         f.close()
                # else:
                #     item1_text = item1_text_ori
            else:
                item1_text = None
            # ------------------------------------------------------------------------------------------------------------------ #
            # Process Item 1A text if filename exists
            if not pd.isna(row['filename_item1a']):
                _item1a_path = os.path.join(ITEM_PATH, str(row['cik']), 'item1a_rf', 'item1a_final', row['filename_item1a'])
                if os.path.exists(_item1a_path):
                    with open(_item1a_path, 'r', encoding='utf-8') as f:
                        item1a_text = f.read()
                        f.close()
                else:
                    item1a_text = None

                ###################### Item 1a: More Section ######################
                if item1a_text == None:
                    item1a_text = _search_more_sections(os.path.join(ITEM_PATH, str(row['cik']), 'item1a_rf', 'item1a_final'), row['filename_item1a'])
                # _item1_path = os.path.join(ITEM_PATH, 'item1a_rf', 'item1a_final', row['filename_item1a'].replace('.txt', '') + '_more_section' + '.txt')
                # if os.path.exists(_item1_path):
                #     with open(_item1_path, 'r', encoding='utf-8') as f:
                #         item1a_text_more_section = f.read()
                #         if item1a_text_ori is None:
                #             item1a_text = item1a_text_more_section
                #         else:
                #             item1a_text = item1a_text_ori + '\n' + item1a_text_more_section
                #         f.close()
                # else:
                #     item1a_text = item1a_text_ori

            else:
                item1a_text = None

            # ------------------------------------------------------------------------------------------------------------------ #
            # Process Item 7 text if filename exists
            if not pd.isna(row['filename_item7']):
                _item7_path = os.path.join(ITEM_PATH, str(row['cik']), 'item7_mda', 'item7_final', row['filename_item7'])
                if os.path.exists(_item7_path):
                    with open(_item7_path, 'r', encoding='utf-8') as f:
                        item7_text = f.read()
                        f.close()
                else:
                    item7_text = None

                ###################### Item 7: More Section ######################
                if item7_text == None:
                    item7_text = _search_more_sections(os.path.join(ITEM_PATH, str(row['cik']), 'item7_mda', 'item7_final'), row['filename_item7'])
            else:
                item7_text = None
            _id = create_id(row['cik'], row['reporting_date'], row['type'])
            list_items.append([_id, row['cik'], row['reporting_date'], row['type'],
                               item1_text,
                               item1a_text,
                               item7_text])
        except Exception as e:
            print(e)
            pass

    df_out = pd.DataFrame(list_items, columns=['_id', 'cik', 'date', 'type', 'item1', 'item1a', 'item7'])
    df_out = df_out[['_id', 'cik', 'date', 'type', 'item1', 'item1a', 'item7']]
    df_out = process_SECfiling_Compstat(df_out, df_compstat, df_out.type.iloc[0])
    df_out.filing_date = df_out.filing_date.astype(str)
    df_out.datadate = df_out.datadate.astype(str).replace('NaT', '')
    df_out = df_out.fillna('')
    df_out = df_out[['_id', 'cik', 'gvkey', 'tic', 'conm', 'filing_date', 'fyear', 'fmonth', 'type', 'item1', 'item1a', 'item7']]
    df_out.dropna(subset=['item1', 'item1a', 'item7'], how='all', inplace=True)
    for i, df_temp in tqdm(df_out.iterrows()):
        common_methods.insert_db_one(df_temp.to_dict(), 'Level2_10K')
    # insert_db(df_out, 'Level2_10K')

def L3_insert_stats(df_compstat):

    def group_files(list_cik_year_output):
        """
        Group files by their base name.

        Args:
            list_cik_year_output (list): List of filenames.

        Returns:
            dict: Dictionary where keys are base filenames and values are lists of full filenames.
        """
        grouped_files = {}
        for file in list_cik_year_output:
            # Split the filename into base name and extension
            base_name, extension = os.path.splitext(file)
            # If the base name is not in the dictionary, add it with an empty list
            if base_name not in grouped_files:
                grouped_files[base_name] = []
            # Append the extension to the list for this base name
            grouped_files[base_name].append(''.join([base_name, extension]))
        return grouped_files

    # def convert_cik(ticker):
    #     """
    #     Convert a ticker symbol to its corresponding CIK code.
    #
    #     Args:
    #         ticker (str): Ticker symbol.
    #
    #     Returns:
    #         str: Corresponding CIK code.
    #     """
    #     for key, value in TickerFile_dict.items():
    #         if value == ticker:
    #             return key
    #     for key, value in TickerFile.items():
    #         if value == ticker:
    #             return key
    L3_STAT_PATH = os.environ['L3_STAT_PATH']
    list_item = os.listdir(L3_STAT_PATH)
    list_results_dict = []
    for item in list_item:
        list_cik = os.listdir(os.path.join(L3_STAT_PATH, item))
        for cik in list_cik:
            list_cik_year = os.listdir(os.path.join(L3_STAT_PATH, item, cik))
            # cik_ = convert_cik(cik)
            for cik_year in list_cik_year:
                date = cik_year.split('-')[1]
                list_cik_year_output = os.listdir(os.path.join(L3_STAT_PATH, item, cik, cik_year))
                grouped_files = group_files(list_cik_year_output)

                # Create a unique ID for the current CIK and date
                _id = create_id(cik, date)

                # Initialize a results dictionary
                results_dict = {"_id": _id, "Cik": cik,"Date": date, "Table_HTML": [],
                                "Table_Fstats": []}
                for key, value in grouped_files.items():
                    html_path = os.path.join(L3_STAT_PATH, item, cik, cik_year, value[0])
                    json_path = os.path.join(L3_STAT_PATH, item, cik, cik_year, value[1])
                    with open(html_path, "r", encoding='utf-8') as f:
                        html_text = f.read()

                    # Check if the JSON file is not empty
                    if os.stat(json_path).st_size != 0:
                        with open(json_path, 'r') as file:
                            json_data = json.loads(file.read())
                        json_string = json.dumps(json_data, indent=4)
                    else:
                        json_string = ''
                    if html_text != '' and json_string != '':
                        results_dict['Table_HTML'].append(html_text)
                        results_dict['Table_Fstats'].append(json_string)
                list_results_dict.append(results_dict)

    insert_db(list_results_dict, 'Level3_10K')





if __name__ == '__main__':
    logger = Log("Start extract Items!").getlog()
    try:
        CompStat_Path = os.environ['COMPSTAT_ANNUAL_PATH']
    except KeyError:
        logger.info('Fail to extract Compstat data, please acquired up-to-date info from WRDS')
        raise KeyError("The environment variable 'COMPSTAT_PATH' is not set, Please acquired up-to-date info from WRDS")
    list_CompStat = os.listdir(CompStat_Path)
    if len(list_CompStat) > 1:
        raise KeyError("Please keep only one csv file in the COMPSTAT_PATH")
    else:
        logger.info('Start reading Compstat data...')
        df_compstat = pd.read_csv(os.path.join(CompStat_Path, list_CompStat[0]))
    df_compstat = preprocess_compstat(df_compstat)
    # with open('comp_cik_full.json', "r") as json_file:
    #     TickerFile = json.load(json_file)
    # TickerFile_dict = {}
    # with open('company_ticker.json', "r") as json_file:
    #     TickerFile_2 = json.load(json_file)
    # for i in TickerFile_2:
    #     TickerFile_dict[TickerFile_2[i]['cik_str']] = TickerFile_2[i]['ticker']
    DB = connect_db()
    # insert_original_text(df_compstat)
    insert_items(df_compstat)
    # L3_insert_stats(df_compstat)