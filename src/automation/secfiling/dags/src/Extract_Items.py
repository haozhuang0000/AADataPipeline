"""
TASK: Extract Items from 10K report
"""
import re
import os
import pandas as pd

## inserting the .../ACPAS/Scripts folder to sys.path to override the official
## python logger library with the custom logger library
import sys 
base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, base_path)
from logger import Log

from tqdm import tqdm
import common_methods
import math
from joblib import Parallel, delayed, parallel_backend
import joblib
import multiprocessing
import datetime

def parse(file1):
    hand = open(file1, encoding='utf-8')
    IDENTITY = ""
    for line in hand:
        line = line.strip()
        if re.findall('^COMPANY CONFORMED NAME:', line):
            k = line.find(':')
            comnam = line[k + 1:]
            comnam = comnam.strip()
            IDENTITY = '<HEADER>\nCOMPANY NAME: ' + str(comnam) + '\n'
            break

    hand = open(file1, encoding='utf-8')
    for line in hand:
        line = line.strip()
        if re.findall('^CENTRAL INDEX KEY:', line):
            k = line.find(':')
            cik = line[k + 1:]
            cik = cik.strip()
            # print(cik)
            IDENTITY = IDENTITY + 'CIK: ' + str(cik) + '\n'
            break

    hand = open(file1, encoding='utf-8')
    for line in hand:
        line = line.strip()
        if re.findall('^STANDARD INDUSTRIAL CLASSIFICATION:', line):
            k = line.find(':')
            sic = line[k + 1:]
            sic = sic.strip()
            siccode = []
            for s in sic:
                if s.isdigit():
                    siccode.append(s)
                    # print(siccode)
            IDENTITY = IDENTITY + 'SIC: ' + ''.join(siccode) + '\n'
            break

    hand = open(file1, encoding='utf-8')
    for line in hand:
        line = line.strip()
        if re.findall('^CONFORMED SUBMISSION TYPE:', line):
            k = line.find(':')
            subtype = line[k + 1:]
            subtype = subtype.strip()
            # print(subtype)
            IDENTITY = IDENTITY + 'FORM TYPE: ' + str(subtype) + '\n'
            break

    hand = open(file1, encoding='utf-8')
    for line in hand:
        line = line.strip()
        if re.findall('^CONFORMED PERIOD OF REPORT:', line):
            k = line.find(':')
            cper = line[k + 1:]
            cper = cper.strip()
            # print(cper)
            IDENTITY = IDENTITY + 'REPORT PERIOD END DATE: ' + str(cper) + '\n'
            break

    hand = open(file1, encoding='utf-8')
    for line in hand:
        line = line.strip()
        if re.findall('^FILED AS OF DATE:', line):
            k = line.find(':')
            fdate = line[k + 1:]
            fdate = fdate.strip()
            # print(fdate)
            IDENTITY = IDENTITY + 'FILE DATE: ' + str(fdate) + '\n' + '</HEADER>\n'
            break

    # with open(file2, 'a') as f:
    #     f.write(str(IDENTITY))
    #     f.close()
    # hand.close()
    return str(fdate), str(cik), str(comnam)
###########################  DELETE HEADER INFORMATION  #######################################

def headerclean(temp):

    ############# Remove SEC Header #########################
    mark0 = 0
    strings1 = ['</SEC-HEADER>', '</IMS-HEADER>']
    hand = open(temp, encoding='utf-8')
    hand.seek(0)
    for x, line in enumerate(hand):
        line = line.strip()
        if any(s in line for s in strings1):
            mark0 = x
            break
    hand.seek(0)

    # newfile = open(temp1, 'w', encoding='utf-8')
    newfile_list = []
    for x, line in enumerate(hand):
        if x > mark0:
            newfile_list.append(line)
            # newfile.write(line)
    hand.close()
    newfile_str = '\n'.join(newfile_list)
    # newfile.close()
    # try:
    # newfile = open(temp1, 'r', encoding='utf-8', errors='ignore')
    # hand_temp = open(temp2, 'w', encoding='utf-8', errors='ignore')
    hand_temp_list = []
    for line in newfile_list:
        if "END PRIVACY-ENHANCED MESSAGE" not in line:
            # hand_temp.write(line)
            hand_temp_list.append(line)

    return ''.join(hand_temp_list)
    # hand_temp.close()
    # newfile.close()
def xbrl_clean(cond1, cond2, str0):
    locations = [0]
    # print(locations)
    placement1 = []
    str0 = str0.lower()
    for m in re.finditer(cond1, str0):
        a = m.start()
        placement1.append(a)
    # print(placement1)

    if placement1 != []:
        placement2 = []
        for m in re.finditer(cond2, str0):
            a = m.end()
            placement2.append(a)
        #    print(placement2)

        len1 = len(placement1)
        placement1.append(len(str0))

        for i in range(len1):
            placement3 = []
            locations.append(placement1[i])
            for j in placement2:
                if (j > placement1[i] and j < placement1[i + 1]):
                    placement3.append(j)
                    break
            if placement3 != []:
                locations.append(placement3[0])
            else:
                locations.append(placement1[i])

    # print(locations)
    return locations
def table_clean(cond1, cond2, str1):
    Items0 = ["item 1", "item1", "item 2", "item2", "item 1a", "item1a"]
    Items1 = ["item 1b", "item1b", "item 3", "item 4", "item 5", "item 6", "item 7", "item 8", "item 9", "item 10",
              "item1", "item3", "item4", "item5", "item6", "item7", "item8", "item9", "item10"]

    str2 = str1.lower()
    placement1 = []
    for m in re.finditer(cond1, str2):
        a = m.start()
        placement1.append(a)
    n = len(placement1)
    placement1.append(len(str2))

    placement2 = []
    for m in re.finditer(cond2, str2):
        a = m.end()
        placement2.append(a)

    if (placement1 != [] and placement2 != []):
        current = str1[0:placement1[0]]

        for i in range(n):
            begin = placement1[i]
            for j in placement2:
                if j > begin:
                    end = j
                    break

            if end == "":
                current = current + str1[begin:placement1[i + 1]]
            else:
                str2 = ""
                str2 = str1[begin:end].lower()
                str2 = str2.replace("&nbsp;", " ")
                str2 = str2.replace("&NBSP;", " ")
                p = re.compile(r'&#\d{1,5};')
                str2 = p.sub("", str2)
                p = re.compile(r'&#.{1,5};')
                str2 = p.sub("", str2)
                if any(s in str2 for s in Items0):
                    if not any(s in str2 for s in Items1):
                        current = current + str2

                current = current + str1[end:placement1[i + 1]]
                end = ""
    else:
        current = str1
    return current
def ASCII_section(str1):
    # with open(temp, 'r', encoding='utf-8', errors='ignore') as f:
    #     str1 = f.read()
    output = str1
    locations_xbrlbig = xbrl_clean("<type>zip", "</document>", output)
    locations_xbrlbig.append(len(output))

    if locations_xbrlbig != []:
        str1 = ""
        if len(locations_xbrlbig) % 2 == 0:
            for i in range(0, len(locations_xbrlbig), 2):
                str1 = str1 + output[locations_xbrlbig[i]:locations_xbrlbig[i + 1]]

    # f.close()
    output=str1
    locations_xbrlbig=xbrl_clean("<type>graphic", "</document>", output)
    locations_xbrlbig.append(len(output))

    if locations_xbrlbig!=[0]:
        str1=""
        if len(locations_xbrlbig)%2==0:
            for i in range(0,len(locations_xbrlbig),2):
                str1=str1+output[locations_xbrlbig[i]:locations_xbrlbig[i+1]]

    output=str1
    locations_xbrlbig=xbrl_clean("<type>excel", "</document>", output)
    locations_xbrlbig.append(len(output))

    if locations_xbrlbig != [0]:
        str1 = ""
        if len(locations_xbrlbig) % 2 == 0:
            for i in range(0, len(locations_xbrlbig), 2):
                str1 = str1 + output[locations_xbrlbig[i]:locations_xbrlbig[i + 1]]

    output = str1
    locations_xbrlbig = xbrl_clean("<type>pdf", "</document>", output)
    locations_xbrlbig.append(len(output))

    if locations_xbrlbig != [0]:
        str1 = ""
        if len(locations_xbrlbig) % 2 == 0:
            for i in range(0, len(locations_xbrlbig), 2):
                str1 = str1 + output[locations_xbrlbig[i]:locations_xbrlbig[i + 1]]

    output = str1
    locations_xbrlbig = xbrl_clean("<type>xml", "</document>", output)
    locations_xbrlbig.append(len(output))

    if locations_xbrlbig != [0]:
        str1 = ""
        if len(locations_xbrlbig) % 2 == 0:
            for i in range(0, len(locations_xbrlbig), 2):
                str1 = str1 + output[locations_xbrlbig[i]:locations_xbrlbig[i + 1]]

    output = str1
    locations_xbrlbig = xbrl_clean("<type>ex", "</document>", output)
    locations_xbrlbig.append(len(output))

    if locations_xbrlbig != [0]:
        str1 = ""
        if len(locations_xbrlbig) % 2 == 0:
            for i in range(0, len(locations_xbrlbig), 2):
                str1 = str1 + output[locations_xbrlbig[i]:locations_xbrlbig[i + 1]]

    return str1
def remove_special(str1):
    ######Remove <DIV>, <TR>, <TD>, and <FONT>###########################
    p = re.compile(r'(<DIV.*?>)|(<DIV\n.*?>)|(<DIV\n\r.*?>)|(<DIV\r\n.*?>)|(<DIV.*?\n.*?>)|(<DIV.*?\n\r.*?>)|(<DIV.*?\r\n.*?>)')
    str1=p.sub("",str1)
    p = re.compile(r'(<div.*?>)|(<div\n.*?>)|(<div\n\r.*?>)|(<div\r\n.*?>)|(<div.*?\n.*?>)|(<div.*?\n\r.*?>)|(<div.*?\r\n.*?>)')
    str1=p.sub("",str1)
    p = re.compile(r'(<TD.*?>)|(<TD\n.*?>)|(<TD\n\r.*?>)|(<TD\r\n.*?>)|(<TD.*?\n.*?>)|(<TD.*?\n\r.*?>)|(<TD.*?\r\n.*?>)')
    str1=p.sub("",str1)
    p = re.compile(r'(<td.*?>)|(<td\n.*?>)|(<td\n\r.*?>)|(<td\r\n.*?>)|(<td.*?\n.*?>)|(<td.*?\n\r.*?>)|(<td.*?\r\n.*?>)')
    str1=p.sub("",str1)
    p = re.compile(r'(<TR.*?>)|(<TR\n.*?>)|(<TR\n\r.*?>)|(<TR\r\n.*?>)|(<TR.*?\n.*?>)|(<TR.*?\n\r.*?>)|(<TR.*?\r\n.*?>)')
    str1=p.sub("",str1)
    p = re.compile(r'(<tr.*?>)|(<tr\n.*?>)|(<tr\n\r.*?>)|(<tr\r\n.*?>)|(<tr.*?\n.*?>)|(<tr.*?\n\r.*?>)|(<tr.*?\r\n.*?>)')
    str1=p.sub("",str1)
    p = re.compile(r'(<FONT.*?>)|(<FONT\n.*?>)|(<FONT\n\r.*?>)|(<FONT\r\n.*?>)|(<FONT.*?\n.*?>)|(<FONT.*?\n\r.*?>)|(<FONT.*?\r\n.*?>)')
    str1=p.sub("",str1)
    p = re.compile(r'(<font.*?>)|(<font\n.*?>)|(<font\n\r.*?>)|(<font\r\n.*?>)|(<font.*?\n.*?>)|(<font.*?\n\r.*?>)|(<font.*?\r\n.*?>)')
    str1=p.sub("",str1)
    p = re.compile(r'(<P.*?>)|(<P\n.*?>)|(<P\n\r.*?>)|(<P\r\n.*?>)|(<P.*?\n.*?>)|(<P.*?\n\r.*?>)|(<P.*?\r\n.*?>)')
    str1=p.sub("",str1)
    p = re.compile(r'(<p.*?>)|(<p\n.*?>)|(<p\n\r.*?>)|(<p\r\n.*?>)|(<p.*?\n.*?>)|(<p.*?\n\r.*?>)|(<p.*?\r\n.*?>)')
    str1=p.sub("",str1)
    str1=str1.replace("</DIV>","")
    str1=str1.replace("</div>","")
    str1=str1.replace("</TR>","")
    str1=str1.replace("</tr>","")
    str1=str1.replace("</TD>","")
    str1=str1.replace("</td>","")
    str1=str1.replace("</FONT>","")
    str1=str1.replace("</font>","")
    str1=str1.replace("</P>","")
    str1=str1.replace("</p>","")

    return str1
def remove_xbrl(str1):
    ############# Remove XBRL Sections #########################
    output = str1
    locations_xbrlsmall = xbrl_clean("<xbrl", "</xbrl.*>", output)
    locations_xbrlsmall.append(len(output))

    if locations_xbrlsmall != [0]:
        str1 = ""
        if len(locations_xbrlsmall) % 2 == 0:
            for i in range(0, len(locations_xbrlsmall), 2):
                str1 = str1 + output[locations_xbrlsmall[i]:locations_xbrlsmall[i + 1]]

    return str1
def remove_newline(str1):
    ############# Remove Newlines and Carriage Returns #########################
    str1 = str1.replace("\r\n"," ")
    p = re.compile(r'<.*?>')
    str1 = p.sub("",str1)

    ############# Remove '<a' and '<hr' and <sup Sections #########################
    str1 = str1.replace("&nbsp;", " ")
    str1 = str1.replace("&NBSP;", " ")
    str1 = str1.replace("&LT;", "LT")
    str1 = str1.replace("&#60;", "LT")
    str1 = str1.replace("&#160;", " ")
    str1 = str1.replace("&AMP;", "&")
    str1 = str1.replace("&amp;", "&")
    str1 = str1.replace("&#38;", "&")
    str1 = str1.replace("&APOS;", "'")
    str1 = str1.replace("&apos;", "'")
    str1 = str1.replace("&#39;", "'")
    str1 = str1.replace('&QUOT;', '"')
    str1 = str1.replace('&quot;', '"')
    str1 = str1.replace('&#34;', '"')
    str1 = str1.replace("\t", " ")
    str1 = str1.replace("\v", "")
    str1 = str1.replace("&#149;", " ")
    str1 = str1.replace("&#224;", "")
    str1 = str1.replace("&#145;", "")
    str1 = str1.replace("&#146;", "")
    str1 = str1.replace("&#147;", "")
    str1 = str1.replace("&#148;", "")
    str1 = str1.replace("&#151;", " ")
    str1 = str1.replace("&#153;", "")
    str1 = str1.replace("&#111;", "")
    str1 = str1.replace("&#153;", "")
    str1 = str1.replace("&#253;", "")
    str1 = str1.replace("&#8217;", "")
    str1 = str1.replace("&#32;", " ")
    str1 = str1.replace("&#174;", "")
    str1 = str1.replace("&#167;", "")
    str1 = str1.replace("&#169;", "")
    str1 = str1.replace("&#8220;", "")
    str1 = str1.replace("&#8221;", "")
    str1 = str1.replace("&rsquo;", "")
    str1 = str1.replace("&lsquo;", "")
    str1 = str1.replace("&sbquo;", "")
    str1 = str1.replace("&bdquo;", "")
    str1 = str1.replace("&ldquo;", "")
    str1 = str1.replace("&rdquo;", "")
    str1 = str1.replace("\'", "")
    p = re.compile(r'&#\d{1,5};')
    str1 = p.sub("", str1)
    p = re.compile(r'&#.{1,5};')
    str1 = p.sub("", str1)
    str1 = str1.replace("_", " ")
    str1 = str1.replace("and/or", "and or")
    str1 = str1.replace("-\n", " ")
    p = re.compile(r'\s*-\s*')
    str1 = p.sub(" ", str1)
    p = re.compile(r'(-|=)\s*')
    str1 = p.sub(" ", str1)
    p = re.compile(r'\s\s*')
    str1 = p.sub(" ", str1)
    p = re.compile(r'(\n\s*){3,}')
    str1 = p.sub("\n\n", str1)
    p = re.compile(r'<.*?>')
    str1 = p.sub("", str1)
    str1 = str1.replace('\n', ' ')
    str1 = " ".join(str1.split())
    str1 = str1.replace('Table of Contents', '')
    str1 = " ".join(str1.split())
    return str1
def retrive_item1_biz(MAIN_PATH, filename, str1):


    filepath1 = os.path.join(MAIN_PATH, "item1business\item1_final")
    if not os.path.exists(filepath1):
        os.makedirs(filepath1)
    item1 = {}
    item1_list = ["item 1\. business", "item 1\.business", "item1\. business", "item1\.business",
                  "item 1. business", "item 1.business", "item1. business", "item1.business",
                  "item 1: business", "item 1:business", "item1: business", "item1:business",
                  "item 1 - business", "item 1 -business", "item 1- business", "item 1-business",
                  "item1 - business", "item1 -business", "item1- business", "item1-business",
                  "item 1 business", "item 1business", "item1 business", "item1business",
                  "item 1\. description of business", "item 1\.description of business",
                  "item1\. description of business", "item1\.description of business",
                  "item 1. description of business", "item 1.description of business", "item1. description of business",
                  "item1.description of business",
                  "item 1: description of business", "item 1:description of business", "item1: description of business",
                  "item1:description of business",
                  "item 1 - description of business", "item 1 -description of business",
                  "item 1- description of business", "item 1-description of business",
                  "item1 - description of business", "item1 -description of business", "item1- description of business",
                  "item1-description of business",
                  "item 1 description of business", "item 1description of business", "item1 description of business",
                  "item1description of business"
                  ]
    for i in range(len(item1_list)):
        item1[i + 1] = item1_list[i]

    item2 = {}

    item2_list = ["item 1a\. risk factor", "item 1a\.risk factor", "item1a\. risk factor", "item1a\.risk factor",
                  "item 1a. risk factor", "item 1a.risk factor", "item1a. risk factor", "item1a.risk factor",
                  "item 1a risk factor", "item 1arisk factor", "item1a risk factor", "item1arisk factor",
                  "item 1a: risk factor", "item 1a:risk factor", "item1a: risk factor", "item1a:risk factor",
                  "item 1a - risk factor", "item 1a -risk factor", "item 1a- risk factor", "item 1a-risk factor",
                  "item1a - risk factor", "item1a -risk factor", "item1a- risk factor", "item1a-risk factor",
                  "item 2\. properties", "item 2\.properties", "item2\. properties", "item2\.properties",
                  "item 2. properties", "item 2.properties", "item2. properties", "item2.properties",
                  "item 2 properties", "item 2properties", "item2 properties", "item2properties",
                  "item 2: properties", "item 2:properties", "item2: properties", "item2:properties",
                  'item 2 - properties', 'item 2 -properties', 'item 2- properties', 'item 2-properties',
                  'item2 - properties', 'item2 -properties', 'item2- properties', 'item2-properties',
                  "item 2\. description of propert", "item 2\.description of propert", "item2\. description of propert",
                  "item2\.description of propert",
                  "item 2. description of propert", "item 2.description of propert", "item2. description of propert",
                  "item2.description of propert",
                  "item 2 description of propert", "item 2description of propert", "item2 description of propert",
                  "item2description of propert",
                  "item 2: description of propert", "item 2:description of propert", "item2: description of propert",
                  "item2:description of propert",
                  "item 2 - description of propert", "item 2 -description of propert", "item 2- description of propert",
                  "item 2-description of propert",
                  "item2 - description of propert", "item2 -description of propert", "item2- description of propert",
                  "item2-description of propert"]

    for i in range(len(item2_list)):
        item2[i + 1] = item2_list[i]

    look = {" see ", " see", " refer to ", " refer to", " included in ", " included in", " contained in ", " in ",
            " in", " contained in", " see“", " see “", "detail in ", "details in", "detailed in ", "detailed in",
            ' under ', ' under', 'criteria of the ', 'criteria of the', 'standard of the', 'standard of the ', ' the',
            ' the ', ' caption ', ' caption', ' of ', ',', ', ', ':', ': ', ' captioned ', ' our ', ' our',
            ' corporations ', ' corporation ', ' and ', ' heading ', 'headings '}
    a = {}
    c = {}

    lstr1 = str1.lower()

    # Search for 'Item 1' sections
    for j in range(1, len(item1_list) + 1):
        a[j] = []
        for m in re.finditer(item1[j], lstr1):
            if not m:
                break
            else:
                substr1 = lstr1[m.start() - 20:m.start()]
                indicator = 0
                for s in look:
                    if substr1.endswith(s):
                        indicator = 1
                        break
                if indicator == 0:
                    b = m.start()
                    a[j].append(b)
                # if not any(s in substr1 for s in look):
                #     #print(substr1)
                #     b=m.start()
                #     a[j].append(b)
    # print(i)

    list1 = []
    for value in a.values():
        for thing1 in value:
            list1.append(thing1)
    list1.sort()
    list1.append(len(lstr1))
    list1 = list(set(list1))
    list1 = sorted(list1)

    # Search for 'Item 1a' and 'Item 2' sections
    for j in range(1, len(item2_list) + 1):
        c[j] = []
        for m in re.finditer(item2[j], lstr1):
            # print('m',m)
            if not m:
                break
            else:
                substr1 = lstr1[m.start() - 20:m.start()]
                # print(substr1)
                indicator = 0
                for s in look:
                    if substr1.endswith(s):
                        indicator = 1
                        break
                if indicator == 0:
                    b = m.start()
                    c[j].append(b)
                    # print('b',b)

                # if not any(s in substr1 for s in look):
                #     #print(substr1)
                #     b=m.start()
                #     c[j].append(b)
    list2 = []
    for value in c.values():
        for thing2 in value:
            list2.append(thing2)
    list2 = list(set(list2))
    list2.sort()

    locations = {}
    if list2 == []:
        logger.info(f"NO item1 business - {filename}")
        return 0
    else:
        if list1 == []:
            logger.info(f"NO item1 business - {filename}")
            return 0
        else:
            for k0 in range(len(list1)):
                locations[k0] = []
                locations[k0].append(list1[k0])
            for k0 in range(len(locations)):
                for item in range(len(list2)):
                    if locations[k0][0] <= list2[item]:
                        locations[k0].append(list2[item])
                        break
                if len(locations[k0]) == 1:
                    del locations[k0]
    # print('locations is:', locations)

    if locations == {}:
        # with open(LOG, 'a') as f:
        logger.info(f"NO item1 business - {filename}")
        return 0
    else:
        new_locations = {}
        i = 0
        for k0 in range(len(locations)):
            substring2 = str1[locations[k0][0]:locations[k0][1]]
            substring3 = substring2.split()
            if len(substring3) > 250:
                new_locations[i] = locations[k0]
                i = i + 1
       # print('new_locations is:', new_locations)
        sections = 0
        if new_locations == {}:
            logger.info(f"NO item1 business - {filename}")
            return 0
        if len(new_locations) == 1:
            k0 = 0
            substring2 = str1[new_locations[k0][0]:new_locations[k0][1]]
            substring3 = substring2.split()
            if len(substring3) > 250:
                # print('find one section!')
                sections = sections + 1
                with open(filepath1 + '/' + filename.replace('.txt', '') + '_item1.txt', 'w', encoding='utf-8') as f:
                    f.write(substring2 + "\n")
                    f.close()
        else:
            # print('find more sections:', len(new_locations))
            for k0 in range(len(new_locations)):
                substring2 = str1[new_locations[k0][0]:new_locations[k0][1]]
                substring3 = substring2.split()
                if len(substring3) > 250:
                    sections = sections + 1
                    with open(filepath1 + '/' + filename.replace('.txt', '') + f'_item1_more_section_{sections}.txt', 'w', encoding='utf-8') as f:
                        # f.write("<SECTION>\n")
                        f.write(substring2 + "\n")
                        #f.write("</SECTION>\n")
                        f.close()
        logger.info(str(filename) + "_" + str('ITEM1'))
        return 1
    # print('Complete parsing file: {file}'.format(file=filename))
def retrive_item1a_rf(MAIN_PATH, filename, str1):

    filepath1 = os.path.join(MAIN_PATH, "item1a_rf\item1a_final")
    if not os.path.exists(filepath1):
        os.makedirs(filepath1)
    itemrf = {}
    itemrf_list = ["item 1a\. risk factor", "item 1a\.risk factor", "item1a\. risk factor", "item1a\.risk factor",
                   "item 1a. risk factor", "item 1a.risk factor", "item1a. risk factor", "item1a.risk factor",
                   "item 1a risk factor", "item 1arisk factor", "item1a risk factor", "item1arisk factor",
                   "item 1a: risk factor", "item 1a:risk factor", "item1a: risk factor", "item1a:risk factor",
                   "item 1a - risk factor", "item 1a -risk factor", "item 1a- risk factor", "item 1a-risk factor",
                   "item1a - risk factor", "item1a -risk factor", "item1a- risk factor", "item1a-risk factor",
                   ]
    for i in range(len(itemrf_list)):
        itemrf[i + 1] = itemrf_list[i]

    item2 = {}
    item2_list = ["item 1b\. unresolved staff comment", "item 1b\.unresolved staff comment",
                  "item1b\. unresolved staff comment", "item1b\.unresolved staff comment",
                  "item 1b\. unresolved", "item 1b\.unresolved", "item1b\. unresolved", "item1b\.unresolved",
                  "item 1b. unresolved staff comment", "item 1b.unresolved staff comment",
                  "item1b. unresolved staff comment", "item1b.unresolved staff comment",
                  "item 1b. unresolved", "item 1b.unresolved", "item1b. unresolved", "item1b.unresolved",
                  "item 1b unresolved staff comment", "item 1bunresolved staff comment",
                  "item1b unresolved staff comment", "item1bunresolved staff comment",
                  "item 1b unresolved", "item 1bunresolved", "item1b unresolved", "item1bunresolved",
                  "item 1b: unresolved staff comment", "item 1b:unresolved staff comment",
                  "item1b: unresolved staff comment", "item1b:unresolved staff comment",
                  "item 1b: unresolved", "item 1b:unresolved", "item1b: unresolved", "item1b:unresolved",
                  "item 1b - unresolved staff comment", "item 1b -unresolved staff comment",
                  "item 1b- unresolved staff comment", "item 1b-unresolved staff comment",
                  "item 1b - unresolved", "item 1b -unresolved", "item 1b- unresolved", "item 1b-unresolved",
                  "item1b - unresolved staff comment", "item1b -unresolved staff comment",
                  "item1b - unresolved staff comment", "item1b-unresolved staff comment",
                  "item 2\. properties", "item 2\.properties", "item2\. properties", "item2\.properties",
                  "item 2. properties", "item 2.properties", "item2. properties", "item2.properties",
                  "item 2 properties", "item 2properties", "item2 properties", "item2properties",
                  "item 2: properties", "item 2:properties", "item2: properties", "item2:properties",
                  'item 2 - properties', 'item 2 -properties', 'item 2- properties', 'item 2-properties',
                  'item2 - properties', 'item2 -properties', 'item2- properties', 'item2-properties',
                  "item 2\. description of propert", "item 2\.description of propert", "item2\. description of propert",
                  "item2\.description of propert",
                  "item 2. description of propert", "item 2.description of propert", "item2. description of propert",
                  "item2.description of propert",
                  "item 2 description of propert", "item 2description of propert", "item2 description of propert",
                  "item2description of propert",
                  "item 2: description of propert", "item 2:description of propert", "item2: description of propert",
                  "item2:description of propert",
                  "item 2 - description of propert", "item 2 -description of propert", "item 2- description of propert",
                  "item 2-description of propert",
                  "item2 - description of propert", "item2 -description of propert", "item2- description of propert",
                  "item2-description of propert"]

    for i in range(len(item2_list)):
        item2[i + 1] = item2_list[i]

    # look={" see "," see"," refer to "," refer to", " included in ", " included in"," contained in "," in "," in"," contained in"," see“"," see “","detail in ","details in","detailed in ", "detailed in",
    #     ' under ',' under','criteria of the ','criteria of the','standard of the','standard of the ',' the',' the ',' caption ',' caption',' of ',',',', ',':',': ',' captioned ',' our ',' our',
    #     ' corporations ',' corporation ',' and ',' heading ','headings '}
    look = {" see ", " see", " refer to ", " refer to", " included in ", " included in", " contained in ", " in ",
            " in", " contained in", " see“", " see “", "detail in ", "details in", "detailed in ", "detailed in",
            ' under ', ' under', 'criteria of the ', 'criteria of the', 'standard of the', 'standard of the ', ' the',
            ' the ', ' caption ', ' caption', ' of ', ',', ', ', ':', ': ', ' captioned ', ' our ', ' our',
            ' corporations ', ' corporation ', ' and ', ' heading ', 'headings '}

    a = {}
    c = {}

    lstr1 = str1.lower()
    for j in range(1, len(itemrf_list) + 1):
        a[j] = []
        for m in re.finditer(itemrf[j], lstr1):
            if not m:
                break
            else:
                substr1 = lstr1[m.start() - 20:m.start()]
                indicator = 0
                for s in look:
                    if substr1.endswith(s):
                        indicator = 1
                        break
                if indicator == 0:
                    b = m.start()
                    a[j].append(b)
                # if not any(s in substr1 for s in look):
                #     #print(substr1)
                #     b=m.start()
                #     a[j].append(b)
    # print(i)

    list1 = []
    for value in a.values():
        for thing1 in value:
            list1.append(thing1)
    list1.sort()
    list1.append(len(lstr1))
    list1 = list(set(list1))
    list1 = sorted(list1)
    # print(list1)
    # print(list1)

    for j in range(1, len(item2_list) + 1):
        c[j] = []
        for m in re.finditer(item2[j], lstr1):
            # print('m',m)
            if not m:
                break
            else:
                substr1 = lstr1[m.start() - 20:m.start()]
                # print(substr1)
                indicator = 0
                for s in look:
                    if substr1.endswith(s):
                        indicator = 1
                        break
                if indicator == 0:
                    b = m.start()
                    c[j].append(b)
                    # print('b',b)

                # if not any(s in substr1 for s in look):
                #     #print(substr1)
                #     b=m.start()
                #     c[j].append(b)
    list2 = []
    for value in c.values():
        for thing2 in value:
            list2.append(thing2)
    list2 = list(set(list2))
    list2.sort()

    locations = {}
    if list2 == []:
        logger.info(f"NO item1a risk factors - {filename}")
        return 0
    else:
        if list1 == []:
            logger.info(f"NO item1a risk factors - {filename}")
            return 0
        else:
            for k0 in range(len(list1)):
                locations[k0] = []
                locations[k0].append(list1[k0])
            for k0 in range(len(locations)):
                for item in range(len(list2)):
                    if locations[k0][0] <= list2[item]:
                        locations[k0].append(list2[item])
                        break
                if len(locations[k0]) == 1:
                    del locations[k0]
    # print('locations is:', locations)

    if locations == {}:
        logger.info(f"NO item1a risk factors - {filename}")
        return 0
    else:
        new_locations = {}
        i = 0
        for k0 in range(len(locations)):
            substring2 = str1[locations[k0][0]:locations[k0][1]]
            substring3 = substring2.split()
            if len(substring3) > 250:
                new_locations[i] = locations[k0]
                i = i + 1
        # print('new_locations is:', new_locations)

        sections = 0
        if new_locations == {}:
            logger.info(f"NO item1a risk factors - {filename}")
            return 0
        if len(new_locations) == 1:
            k0 = 0
            # for k0 in range(len(locations)):
            substring2 = str1[new_locations[k0][0]:new_locations[k0][1]]
            substring3 = substring2.split()
            if len(substring3) > 250:
                # print('find one section!')
                sections = sections + 1
                # with open(Filer,'a') as f:
                #     # f.write("<SECTION>\n")
                #     f.write(substring2+"\n")
                #     #f.write("</SECTION>\n")
                #     f.close()
                with open(filepath1 + '/' + filename.replace('.txt', '') + '_riskfactor.txt', 'w', encoding='utf-8') as f:
                    f.write(substring2 + "\n")
                    f.close()
        else:
            # print('find more sections:', len(new_locations))
            for k0 in range(len(new_locations)):
                substring2 = str1[new_locations[k0][0]:new_locations[k0][1]]
                substring3 = substring2.split()
                if len(substring3) > 250:
                    sections = sections + 1
                    with open(filepath1 + '/' + filename.replace('.txt', '') + f'_riskfactor_more_section_{sections}.txt', 'w', encoding='utf-8') as f:
                        # f.write("<SECTION>\n")
                        f.write(substring2 + "\n")
                        #f.write("</SECTION>\n")
                        f.close()

        logger.info(str(filename) + "_" + str('ITEM1A'))
        return 1
    # print('Complete parsing file: {file}'.format(file=filename))
def retrive_item7_mda(MAIN_PATH, filename, str1):

    filepath1 = os.path.join(MAIN_PATH, r"item7_mda\item7_final")
    if not os.path.exists(filepath1):
        os.makedirs(filepath1)
    item7 = {}
    item7_list = ["item 7\. managements discussion and analysis", "item 7\.managements discussion and analysis",
                  "item7\. managements discussion and analysis", "item7\.managements discussion and analysis",
                  "item 7. managements discussion and analysis", "item 7.managements discussion and analysis",
                  "item7. managements discussion and analysis", "item7.managements discussion and analysis",
                  "item 7: managements discussion and analysis", "item 7:managements discussion and analysis",
                  "item7: managements discussion and analysis", "item7:managements discussion and analysis",
                  "item 7 - managements discussion and analysis", "item 7 -managements discussion and analysis",
                  "item 7- managements discussion and analysis", "item 7-managements discussion and analysis",
                  "item7 - managements discussion and analysis", "item7 -managements discussion and analysis",
                  "item7- managements discussion and analysis", "item7-managements discussion and analysis",
                  "item 7 managements discussion and analysis", "item 7managements discussion and analysis",
                  "item7 managements discussion and analysis", "item7managements discussion and analysis",
                  "item 7\. management discussion and analysis", "item 7\.management discussion and analysis",
                  "item7\. management discussion and analysis", "item7\.management discussion and analysis",
                  "item 7. management discussion and analysis", "item 7.management discussion and analysis",
                  "item7. management discussion and analysis", "item7.management discussion and analysis",
                  "item 7: management discussion and analysis", "item 7:management discussion and analysis",
                  "item7: management discussion and analysis", "item7:management discussion and analysis",
                  "item 7 - management discussion and analysis", "item 7 -management discussion and analysis",
                  "item 7- management discussion and analysis", "item 7-management discussion and analysis",
                  "item7 - management discussion and analysis", "item7 -management discussion and analysis",
                  "item7- management discussion and analysis", "item7-management discussion and analysis",
                  "item 7 management discussion and analysis", "item 7management discussion and analysis",
                  "item7 management discussion and analysis", "item7management discussion and analysis",
                  "item 7\. management’s discussion and analysis", "item 7. management’s discussion and analysis"]
    for i in range(len(item7_list)):
        item7[i + 1] = item7_list[i]

    item8 = {}
    item8_list = ["item 8\. financial statement", "item 8\.financial statement", "item8\. financial statement",
                  "item8\.financial statement",
                  "item 8. financial statement", "item 8.financial statement", "item8. financial statement",
                  "item8.financial statement",
                  "item 8: financial statement", "item 8:financial statement", "item8: financial statement",
                  "item8:financial statement",
                  "item 8 - financial statement", "item 8 -financial statement", "item 8- financial statement",
                  "item 8-financial statement",
                  "item8 - financial statement", "item8 -financial statement", "item8- financial statement",
                  "item8-financial statement",
                  "item 8 financial statement", "item 8financial statement", "item8 financial statement",
                  "item8financial statement",
                  "item 8\. consolidated financial statement", "item 8\.consolidated financial statement",
                  "item8\. consolidated financial statement", "item8\.consolidated financial statement",
                  "item 8. consolidated financial statement", "item 8.consolidated financial statement",
                  "item8. consolidated financial statement", "item8.consolidated financial statement",
                  "item 8: consolidated financial statement", "item 8:consolidated financial statement",
                  "item8: consolidated financial statement", "item8:consolidated financial statement",
                  "item 8 - consolidated financial statement", "item 8 -consolidated financial statement",
                  "item 8- consolidated financial statement", "item 8-consolidated financial statement",
                  "item8 - consolidated financial statement", "item8 -consolidated financial statement",
                  "item8- consolidated financial statement", "item8-consolidated financial statement",
                  "item 8 consolidated financial statement", "item 8consolidated financial statement",
                  "item8 consolidated financial statement", "item8consolidated financial statement",
                  "item 8\. audited financial statement", "item 8\.audited financial statement",
                  "item8\. audited financial statement", "item8\.audited financial statement",
                  "item 8. audited financial statement", "item 8.audited financial statement",
                  "item8. audited financial statement", "item8.audited financial statement",
                  "item 8: audited financial statement", "item 8:audited financial statement",
                  "item8: audited financial statement", "item8:audited financial statement",
                  "item 8 - audited financial statement", "item 8 -audited financial statement",
                  "item 8- audited financial statement", "item 8-audited financial statement",
                  "item8 - audited financial statement", "item8 -audited financial statement",
                  "item8- audited financial statement", "item8-audited financial statement",
                  "item 8 audited financial statement", "item 8audited financial statement",
                  "item8 audited financial statement", "item8audited financial statement",
                  "item 8a\. financial statement", "item 8a\.financial statement", "item8a\. financial statement",
                  "item8a\.financial statement",
                  "item 8a. financial statement", "item 8a.financial statement", "item8a. financial statement",
                  "item8a.financial statement",
                  "item 8a: financial statement", "item 8a:financial statement", "item8a: financial statement",
                  "item8a:financial statement",
                  "item 8a - financial statement", "item 8a -financial statement", "item 8a- financial statement",
                  "item 8a-financial statement",
                  "item8a - financial statement", "item8a -financial statement", "item8a- financial statement",
                  "item8a-financial statement",
                  "item 8a financial statement", "item 8afinancial statement", "item8a financial statement",
                  "item8afinancial statement",
                  "item 8a\. consolidated financial statement", "item 8a\.consolidated financial statement",
                  "item8a\. consolidated financial statement", "item8a\.consolidated financial statement",
                  "item 8a. consolidated financial statement", "item 8a.consolidated financial statement",
                  "item8a. consolidated financial statement", "item8a.consolidated financial statement",
                  "item 8a: consolidated financial statement", "item 8a:consolidated financial statement",
                  "item8a: consolidated financial statement", "item8a:consolidated financial statement",
                  "item 8a - consolidated financial statement", "item 8a -consolidated financial statement",
                  "item 8a- consolidated financial statement", "item 8a-consolidated financial statement",
                  "item8a - consolidated financial statement", "item8a -consolidated financial statement",
                  "item8a- consolidated financial statement", "item8a-consolidated financial statement",
                  "item 8a consolidated financial statement", "item 8aconsolidated financial statement",
                  "item8a consolidated financial statement", "item8aconsolidated financial statement",
                  "item 8a\. audited financial statement", "item 8a\.audited financial statement",
                  "item8a\. audited financial statement", "item8a\.audited financial statement",
                  "item 8a. audited financial statement", "item 8a.audited financial statement",
                  "item8a. audited financial statement", "item8a.audited financial statement",
                  "item 8a: audited financial statement", "item 8a:audited financial statement",
                  "item8a: audited financial statement", "item8a:financial statement",
                  "item 8a - audited financial statement", "item 8a -audited financial statement",
                  "item 8a- audited financial statement", "item 8a-audited financial statement",
                  "item8a - audited financial statement", "item8a -audited financial statement",
                  "item8a- financial statement", "item8a-audited financial statement",
                  "item 8a audited financial statement", "item 8aaudited financial statement",
                  "item8a audited financial statement", "item8aaudited financial statement"]

    for i in range(len(item8_list)):
        item8[i + 1] = item8_list[i]

    look = {" see ", " see", " refer to ", " refer to", " included in ", " included in", " contained in ", " in ",
            " in", " contained in", " see“", " see “", "detail in ", "details in", "detailed in ", "detailed in",
            ' under ', ' under', 'criteria of the ', 'criteria of the', 'standard of the', 'standard of the ', ' the',
            ' the ', ' caption ', ' caption', ' of ', ',', ', ', ':', ': ', ' captioned ', ' our ', ' our',
            ' corporations ', ' corporation ', ' and ', ' heading ', 'headings '}

    a = {}
    c = {}

    lstr1 = str1.lower()
    for j in range(1, len(item7_list) + 1):
        a[j] = []
        for m in re.finditer(item7[j], lstr1):
            if not m:
                break
            else:
                substr1 = lstr1[m.start() - 20:m.start()]
                indicator = 0
                for s in look:
                    if substr1.endswith(s):
                        indicator = 1
                        break
                if indicator == 0:
                    b = m.start()
                    a[j].append(b)

    list1 = []
    for value in a.values():
        for thing1 in value:
            list1.append(thing1)
    list1.sort()
    list1.append(len(lstr1))
    list1 = list(set(list1))
    list1 = sorted(list1)

    for j in range(1, len(item8_list) + 1):
        c[j] = []
        for m in re.finditer(item8[j], lstr1):
            if not m:
                break
            else:
                substr1 = lstr1[m.start() - 20:m.start()]
                indicator = 0
                for s in look:
                    if substr1.endswith(s):
                        indicator = 1
                        break
                if indicator == 0:
                    b = m.start()
                    c[j].append(b)
    list2 = []
    for value in c.values():
        for thing2 in value:
            list2.append(thing2)
    list2 = list(set(list2))
    list2.sort()

    locations = {}
    if list2 == []:
        logger.info(f"NO MD&A - {filename}")
        return 0
    else:
        if list1 == []:
            logger.info(f"NO MD&A - {filename}")
            return 0
        else:
            for k0 in range(len(list1)):
                locations[k0] = []
                locations[k0].append(list1[k0])
            for k0 in range(len(locations)):
                for item in range(len(list2)):
                    if locations[k0][0] <= list2[item]:
                        locations[k0].append(list2[item])
                        break
                if len(locations[k0]) == 1:
                    del locations[k0]

    if locations == {}:
        logger.info(f"NO MD&A - {filename}")
        return 0
    else:
        new_locations = {}
        i = 0
        for k0 in range(len(locations)):
            substring2 = str1[locations[k0][0]:locations[k0][1]]
            substring3 = substring2.split()
            if len(substring3) > 250:
                new_locations[i] = locations[k0]
                i = i + 1
        # print('new_locations is:', new_locations)
        sections = 0
        if new_locations == {}:
            logger.info(f"NO MD&A - {filename}")
            return 0
        if len(new_locations) == 1:
            k0 = 0
            substring2 = str1[new_locations[k0][0]:new_locations[k0][1]]
            substring3 = substring2.split()
            if len(substring3) > 250:
                # print('find one section!')
                sections = sections + 1
                with open(filepath1 + '/' + filename.replace('.txt', '') + '_item7.txt', 'w', encoding='utf-8') as f:
                    f.write(substring2 + "\n")
                    f.close()
        else:
            # print('find more sections:', len(new_locations))
            for k0 in range(len(new_locations)):
                substring2 = str1[new_locations[k0][0]:new_locations[k0][1]]
                substring3 = substring2.split()
                if len(substring3) > 250:
                    sections = sections + 1
                    with open(filepath1 + '/' + filename.replace('.txt', '') + f'_item7_more_section_{sections}.txt', 'w', encoding='utf-8') as f:
                        # f.write("<SECTION>\n")
                        f.write(substring2 + "\n")
                        # #f.write("</SECTION>\n")
                        f.close()

        logger.info(str(filename) + "_" + str('ITEM7'))
        return 1

    # print('Complete parsing file: {file}'.format(file=filename))

def run(cik):

    list_summary = []
    # for cik in ciks:
    cik_path = os.path.join(MAIN_PATH, cik)
    list_comp_year_txt = os.listdir(cik_path)
    for comp_year_txt in list_comp_year_txt:
        if comp_year_txt.endswith('.txt'):
            if 'header' not in str(comp_year_txt):
                try:
                    comp_year_path = os.path.join(MAIN_PATH, cik, comp_year_txt)
                    with open(comp_year_path, 'r', encoding='utf-8') as f:
                        text = f.read()
                    if len(text) <= 10000:
                        # comp_year_path = '_fulltxt.'.join(comp_year_path.split('.'))
                        continue
                    f.close()
                    date, cik, comnam = parse(comp_year_path)
                    if not os.path.exists(os.path.join(MAIN_PATH, 'temp_path')):
                        os.makedirs(os.path.join(MAIN_PATH, 'temp_path'))
                    # new_file_path = os.path.join(MAIN_PATH, 'temp_path', 'new_file.txt')
                    # new_file_temp = os.path.join(MAIN_PATH, 'temp_path', 'new_file_temp.txt')
                    str1 = headerclean(comp_year_path)

                    str1 = ASCII_section(str1)
                    str1 = remove_special(str1)
                    str1 = remove_xbrl(str1)
                    str1 = remove_newline(str1)

                    filename = comp_year_txt.replace('.txt', '')
                    dummy_item1 = retrive_item1_biz(os.path.join(OUTPUT_PATH, str(int(cik))), f'{filename}.txt', str1)
                    dummy_item1a = retrive_item1a_rf(os.path.join(OUTPUT_PATH, str(int(cik))), f'{filename}.txt', str1)
                    dummy_item7 = retrive_item7_mda(os.path.join(OUTPUT_PATH, str(int(cik))), f'{filename}.txt', str1)

                    cik, date, type = filename.split('_')[0], filename.split('_')[1], filename.split('_')[2]
                    list_temp = [str(cik), date, type, comnam, f'{filename}.txt', dummy_item1, dummy_item1a, dummy_item7]
                    # df = pd.DataFrame(list_summary, columns=["cik", "tic", "reporting_date", "type", "company_name", "filename"])
                    list_summary.append(list_temp)
                except:
                    cik, date, type = filename.split('_')[0], filename.split('_')[1], filename.split('_')[2]
                    list_temp = [str(cik), date, type, comnam, f'{filename}.txt', -1, -1, -1]
                    list_summary.append(list_temp)
                    logger.info(
                        f'Encountered errors when dealing with cik: {cik} - txt_file: {comp_year_txt}')
                    pass
    return list_summary

def create_items_filename(file_name):

    df = pd.read_csv(os.path.join(SUMMARY_PATH, file_name))
    def _check_dummy(x, type):
        if type == 'item1':
            if x.dummy_item1 == 1:
                return x.filename.split('.txt')[0] + '_item1' + '.txt'
            else:
                return None
        if type == 'item1a':
            if x.dummy_item1a == 1:
                return x.filename.split('.txt')[0] + '_riskfactor' + '.txt'
            else:
                return None
        if type == 'item7':
            if x.dummy_item7 == 1:
                return x.filename.split('.txt')[0] + '_item7' + '.txt'
            else:
                return None

    df['filename_item1'] = df.apply(lambda x: _check_dummy(x, 'item1'), axis=1)
    df['filename_item1a'] = df.apply(lambda x: _check_dummy(x, 'item1a'), axis=1)
    df['filename_item7'] = df.apply(lambda x: _check_dummy(x, 'item7'), axis=1)
    df.to_csv(os.path.join(SUMMARY_PATH, f'10-K_items_summary_final_{cur_time}.csv'), index=False)


if __name__ == '__main__':

    logger = Log("Start extract Items!").getlog()
    OUTPUT_PATH = os.environ['OUTPUT_ITEM_PATH']
    SUMMARY_PATH = os.environ['SUMMARY_PATH']
    MAIN_PATH = os.environ['TEXT_PATH']
    list_cik = os.listdir(MAIN_PATH)
    list_cik = [i for i in list_cik if i != 'temp_path']
    with parallel_backend('threading', n_jobs=15):
        results = Parallel(n_jobs=15)(delayed(run)(cik) for cik in tqdm(list_cik[:15]))

    results_df = []
    for i in results:
        df_temp = pd.DataFrame(i, columns=["cik", "reporting_date", "type", "company_name", "filename", 'dummy_item1',
                                           'dummy_item1a', 'dummy_item7'])
        results_df.append(df_temp)
    df_summary = pd.concat(results_df)
    cur_time = str(datetime.datetime.now()).replace(':', '').replace(' ', '_')
    file_name = f"10-K_items_summary_{cur_time}.csv"
    df_summary.to_csv(os.path.join(SUMMARY_PATH, file_name), index=False)
    create_items_filename(file_name)

    # results = []
    # for cik in tqdm(list_cik):
    #     summary = run(cik)
    #     results.append(summary)
    # cur_time = str(datetime.datetime.now()).replace(':', '').replace(' ', '_')
    #
    # results_df = []
    # for i in results:
    #     df_temp = pd.DataFrame(i,
    #                            columns=["cik", "reporting_date", "type", "company_name", "filename", 'dummy_item1',
    #                                     'dummy_item1a', 'dummy_item7'])
    #     results_df.append(df_temp)
    # df_summary = pd.concat(results_df)
    # file_name = f"10-K_items_summary_{cur_time}.csv"
    # df_summary.to_csv(os.path.join(SUMMARY_PATH, file_name), index=False)
    # create_items_filename(file_name)