import sys
import os
from os import listdir
import time
from time import sleep
from random import randint
import csv
import re
from collections import defaultdict
import cfg
from cfg import create_logger
import requests

sys.path.append('..')
reload(sys)
sys.setdefaultencoding('utf-8')
columns = defaultdict(list)
##########################################################################
### Create Logger
logger = create_logger()
##########################################################################
website_name = 'Amazon_sallers'
##########################################################################
### Time
start_date = time.strftime('%Y-%m-%d')
start_time = time.strftime('%H-%M')
##########################################################################
### CONTROL
control_folder_path = os.path.join(cfg.main_path, website_name, "Companies CONTROL")
if not os.path.exists(control_folder_path):
    os.makedirs(control_folder_path)
all_control_files = listdir(control_folder_path)
control_list = []
for one_control_file in all_control_files:
    logger.info("one CONTROL file   "+one_control_file)
    full_path_for_control_file = os.path.join(control_folder_path, one_control_file)
    logger.info(full_path_for_control_file)
    with open(full_path_for_control_file, 'rU') as f2:
        reader2 = csv.reader(f2)
        for row2 in reader2:
            # logger.info(row2)
            row2 = str(row2).replace("[", "").replace("]", "").replace("'", "")
            # control_list.append(row2)
logger.info("CONTROL LIST:   "+str(len(control_list)))
header = "URL"
file_name = "Links companies CONTROL"
output_file2 = open(full_path_for_control_file, 'ab')
info_writer2 = csv.writer(output_file2, delimiter=',', quoting=csv.QUOTE_ALL)
info_writer2.writerow([header])
##########################################################################
### IN fileLinks control
in_folder_path = os.path.join(cfg.main_path, website_name, 'Links letters')
all_in_files = listdir(in_folder_path)
##########################################################################
### OUT file
out_folder_path = os.path.join(cfg.main_path, website_name, "Links companies")
logger.info(out_folder_path)
if not os.path.exists(out_folder_path):
    os.makedirs(out_folder_path)
##########################################################################
### PROXIES
http_proxy = "5.79.66.2:13010"
https_proxy = "5.79.66.2:13010"
ftp_proxy = "5.79.66.2:13010"

proxyDict = {
    "http": http_proxy,
    "https": https_proxy,
    "ftp": ftp_proxy
}
hdr = {'user-agent': 'my-app/0.0.1'}
###############################################################################################
###############################################################################################
###############################################################################################
###############################################################################################
logger.info(" ############################### START TIME:  " +str(start_time) +"################################## ")

start_file = 0
end_file = start_file + 1
link_start = 0
link_end = link_start + 1000000000
for one_file in all_in_files[start_file:end_file]:
    logger.info("IN FILE:   "+one_file)
    full_path_in_file = os.path.join(in_folder_path, one_file)
    logger.info(full_path_in_file)
    list_urls = ''
    # logger.info(list_urls)
    logger.info('++++++++++++')
    with open(full_path_in_file) as f:
        reader = csv.DictReader(f)
        for row in reader:
            for (k, v) in row.items():
                columns[k].append(v)
    list_urls = columns["URL"]
    logger.info(len(list_urls))
    # logger.info(list_urls)
    link_number = link_start
    new_add_list = []

for url in list_urls[link_start:link_end]:
    logger.info(str(link_number)+' URL  '+url)
    link_number += 1
    if url == 'URL':
        continue
    link_number +=1
    repeat_num = 0
    if 'baby' in url or 'clothing' in url:
        Main_Department = 'baby'
    if 'kitchen' in url:
        Main_Department = 'kitchen'
    logger.info(Main_Department)

    if 'DE' in one_file:
        logger.info("Germany")
        domain_part = 'de'
    else:
        pass

    if 'JP' in one_file:
        logger.info("Japan")
        domain_part = 'jp'
    else:
        pass

    if 'UK' in one_file:
        logger.info("United Kingdom")
        domain_part = 'co.uk'
    else:
        pass

    if 'IT' in one_file:
        logger.info("Italy")
        domain_part = 'it'
    else:
        pass

    if 'FR' in one_file:
        logger.info("France")
        domain_part = 'fr'
    else:
        pass

    if 'CA' in one_file:
        logger.info("Canada")
        domain_part = 'ca'
    else:
        pass

    if 'ES' in one_file:
        logger.info("Spanija")
        domain_part = 'es'
    else:
        pass

    if 'mx' in one_file:
        logger.info("Mexico")
        domain_part = 'com.mx'
    else:
        pass
    # logger.info(domain_part)

    if 'IN' in one_file:
        logger.info("India")
        domain_part = 'in'
    else:
        pass
    logger.info(domain_part)

    file_name_final = "Links sellers from partial letters NEW "+str(domain_part)+' '+Main_Department+' .csv'
    logger.info("file_name_final      "+file_name_final)
    output_file = open(out_folder_path + '/' + file_name_final, 'ab')
    info_writer = csv.writer(output_file, delimiter=',', quoting=csv.QUOTE_ALL)
    info_writer.writerow([header])

    while range(0,10,1):
        time.sleep(1)
        if start_file == 0:
            try:
                page = requests.get(url, headers=hdr, proxies=proxyDict)  #
                page_status_code = page.status_code
                logger.info(page_status_code)
            except:
                page_status_code = ''
                pass
            page_source = page.text
        else:
            try:
                sleep(randint(1,3))
                page = requests.get(url, headers=hdr)  #
                page_status_code = page.status_code
                logger.info(page_status_code)
            except:
                page_status_code = ''
                pass
        page_source = page.text
        # logger.info(page_source)
        page_source_short = page_source.replace('<a class="', '\n<a class="').replace('</a>', '</a>\n').replace('<span class=', '\n<span class=')
        # logger.info(page_source_short)

        all_sellers = re.findall('(<a class=".*)', page_source_short)
        logger.info("NUMBER OF SELLERS    "+str(len(all_sellers)))
        if len(all_sellers) != 0:
            logger.error("FIND SELLERS")
            break
        else:
            logger.info("REPEAT")
            repeat_num +=1
            if repeat_num == 10:
                break
            else:
                pass

    if len(all_sellers) == 0:
        logger.info('links_dont_open_well' )+url

    new_sellers = []
    for one_seller in all_sellers:
        try:
            # logger.info(one_seller)
            seller_info = re.search('href="/s/(.*)', one_seller).group(1)
            # logger.info(seller_info)
        except:
            try:
                # logger.info(one_seller)
                seller_info = re.search('(href.*Cp_6%3A.*">)', one_seller).group(1)
                # logger.info(seller_info)
            except:
                logger.info("link is not good")
                continue
        try:
            seller_ID = re.search('Cp_6%3A(.*)&amp;bbn', one_seller).group(1)
            # logger.info("seller_ID          "+seller_ID)
        except:
            try:
                seller_ID = re.search('Cp_6%3A(.*)">', one_seller).group(1)
                # logger.info("seller_ID          "+seller_ID)
            except:
                seller_ID = ''
                logger.error("PROBLEM ID")
                logger.info(seller_info)
                logger.info(one_seller)
                logger.info("~~~~~~~~~***************")
        try:
            seller_url = 'https://www.amazon.'+domain_part+'/sp?&seller='+seller_ID+'&tab='
            # logger.info("seller_url            "+seller_url)
        except:
            seller_url = ''
            logger.error("PROBLEM seller_url")
            logger.info(seller_ID)
            logger.info(seller_info)
            logger.info(one_seller)
            logger.info("~~~~~~~~~")

        if seller_ID in control_list or [seller_ID] in control_list:
            # logger.info("DUPLICATE")
            pass
        else:
            info_writer.writerow([seller_url])
            info_writer2.writerow([seller_ID])
            control_list.append(seller_ID)
            new_sellers.append(seller_ID)
    logger.info("NEW add  "+str(len(new_sellers)))
    logger.info("  ----------------------------------------  ")
