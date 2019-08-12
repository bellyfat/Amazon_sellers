# -*- coding: utf-8 -*-
# -*- coding: iso-8859-1 -*-
# -*- coding: iso-8859-5 -*-
# -*- coding: iso-8859-15 -*-
# -*- coding: latin-1 -*-

import sys
import os
from os import listdir
import time
import mysql.connector
import csv
import re
from collections import defaultdict
import cfg
from cfg import create_logger
import requests
from HTMLParser import HTMLParser
from time import sleep
from random import randint
import mysql.connector

h = HTMLParser()
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
mydb = mysql.connector.connect(
    host="localhost",
    user='root',
    passwd='9713',
    database='amazonsellers'
)
print mydb
mycursor = mydb.cursor(buffered=True)

# mycursor.execute("CREATE DATABASE amazonSellers")
mycursor.execute("SHOW DATABASES")
for db in mycursor:
    print db
try:
    mycursor.execute("CREATE TABLE amazon_seller_contact_information (Marketplace VARCHAR(255), Main_Department VARCHAR(255), Sub_Department VARCHAR(255), Seller_Name VARCHAR(255), Company_Name VARCHAR(255), Business_Address TEXT, Service_Address TEXT, Company_Type VARCHAR(255), Register_No VARCHAR(255), VAT_ID VARCHAR(255), Business_Representative_1 TEXT, Business_Representative_2 TEXT, Email_ID VARCHAR(255), Contact_Number VARCHAR(255), PHONE VARCHAR(255), Fax VARCHAR(255), Feedback_30d VARCHAR(255), Feedback_90d VARCHAR(255), Feedback_365d VARCHAR(255), Feedback_all_time VARCHAR(255), Pos_Feedback_30d_percentage VARCHAR(255),Pos_Feedback_90d_percentage VARCHAR(255), Pos_Feedback_365d_percentage VARCHAR(255), Pos_Feedback_All_time_percentage VARCHAR(255), About_Seller_Description TEXT, Store_URL VARCHAR(255), Created_at VARCHAR(255), Updated_at VARCHAR(255))" )
except:
    pass
mycursor.execute("SHOW TABLES")

try:
    sqlFormula = "INSERT INTO amazon_seller_contact_information (Marketplace, Main_Department, Sub_Department, Seller_Name, Company_Name, Business_Address, Service_Address, Company_Type, Register_No, VAT_ID, Business_Representative_1, Business_Representative_2, Email_ID, Contact_Number, PHONE, Fax, Feedback_30d, Feedback_90d, Feedback_365d, Feedback_all_time, Pos_Feedback_30d_percentage, Pos_Feedback_90d_percentage, Pos_Feedback_365d_percentage, Pos_Feedback_All_time_percentage, About_Seller_Description, Store_URL, Created_at, Updated_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    logger.info('good formula')
except:
    logger.error("FORMULA PROBLEM")
##########################################################################
### CONTROL
control_folder_path = os.path.join(cfg.main_path, website_name, "Info sellers CONTROL")
if not os.path.exists(control_folder_path):
    os.makedirs(control_folder_path)
all_control_files = listdir(control_folder_path)
control_list = []
for one_control_file in all_control_files:
    logger.info("one CONTROL file   " + one_control_file)
    full_path_for_control_file = os.path.join(control_folder_path, one_control_file)
    logger.info(full_path_for_control_file)
    logger.info(full_path_for_control_file)

    with open(full_path_for_control_file, 'rU') as f2:
        reader2 = csv.reader(f2)
        for row2 in reader2:
            row2 = str(row2).replace("[", "").replace("]", "").replace("'", "")
            # logger.info(row2)
            control_list.append(row2)
logger.info("CONTROL LIST:   " + str(len(control_list)))

header = "URL"
file_name = "Info sellers CONTROL"
output_file2 = open(full_path_for_control_file, 'ab')
info_writer2 = csv.writer(output_file2, quoting=csv.QUOTE_ALL)  # , delimiter=','
# info_writer2.writerow(header)
##########################################################################
### IN file
in_folder_path = os.path.join(cfg.main_path, website_name, 'Links companies')
all_in_files = listdir(in_folder_path)
##########################################################################
### OUT file
out_folder_path = os.path.join(cfg.main_path, website_name, "Info sellers")
logger.info(out_folder_path)
if not os.path.exists(out_folder_path):
    os.makedirs(out_folder_path)
header = ["Marketplace", "Main_Department", "Sub_Department", "Seller_Name", "Company_Name", "Business_Address",
          "Service_Address", "Company_Type", "Register_No", "VAT_ID", "Business_Representative_1",
          "Business_Representative_2", "Email_ID", "Contact_Number", "Feedback_30d", "Feedback_90d", "Feedback_365d",
          "Feedback_all_time", "Pos_Feedback_30d_percentage", "Pos_Feedback_90d_percentage",
          "Pos_Feedback_365d_percentage", "Pos_Feedback_All_time_percentage", "About_Seller_Description", "Store_URL",
          "ALL PHONE", "Fax"]  #
##########################################################################
### PROXIES
http_proxy = "5.79.66.2:13010"
https_proxy = "5.79.66.2:13010"
ftp_proxy = "69.30.240.226:15001"
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
### Time
start_time = time.strftime('%Y-%m-%d')
logger.info(" ############################### START TIME:  " + str(start_time) + "################################## ")

start_file = 3    #171981
# start_file = 2    #17488
end_file = start_file + 1
link_start = 17000
link_end = link_start + 1000000000000

for one_file in all_in_files[start_file:end_file]:
    logger.info("IN FILE:   " + one_file)
    full_path_in_file = os.path.join(in_folder_path, one_file)

    logger.info(full_path_in_file)
    part_of_in_file = re.search('Links.(.*).csv', one_file).group(1)
    file_name = "Info Sellers windows  " + part_of_in_file + '' + str(link_start) + ' --- ' + str(link_end)
    output_file = open(out_folder_path + '/' + file_name + '.csv', 'w')
    info_writer = csv.writer(output_file, quoting=csv.QUOTE_ALL)  # , delimiter=','
    info_writer.writerow(header)

    with open(full_path_in_file) as f:
        reader = csv.DictReader(f)
        for row in reader:
            for (k, v) in row.items():
                columns[k].append(v)
    list_urls = columns["URL"]
    logger.info(len(list_urls))
    link_number = link_start
    for url in list_urls[link_start:link_end]:  #
        link_number += 1
        if url in control_list or url == 'URL':
            logger.info("------------------------------------ OLD URL")
            continue
        logger.info(url)
        if 'www.amazon.de' in url:
            logger.info("Germany")
            Marketplace = 'DE'
        else:
            pass

        if 'www.amazon.co.jp' in url:
            logger.info("Japan")
            Marketplace = 'JP'
        else:
            pass

        if  'www.amazon.co.uk' in url:
            logger.info("United Kingdom")
            Marketplace = 'UK'
        else:
            pass

        if  'www.amazon.it' in url:
            logger.info("Italy")
            Marketplace = 'IT'
        else:
            pass

        if  'www.amazon.fr' in url:
            logger.info("France")
            Marketplace = 'FR'
        else:
            pass

        if  'www.amazon.ca' in url:
            logger.info("Canada")
            Marketplace = 'CA'
        else:
            pass

        if  'www.amazon.es' in url:
            logger.info("Spanija")
            Marketplace = 'ES'
        else:
            pass

        if  'www.amazon.com.mx' in url:
            logger.info("Mexico")
            Marketplace = 'MX'
        else:
            pass

        if  'www.amazon.in' in url:
            logger.info("India")
            Marketplace = 'IN'
        else:
            pass

        if  'www.amazon.com' in url:
            logger.info("United States")
            Marketplace = 'US'
        else:
            pass
        logger.info("Marketplace     "+Marketplace)


        if 'baby' in one_file or 'Baby' in one_file:
            Main_Department = 'baby'
        if 'kitchen' in one_file or 'Kitchen' in one_file:
            Main_Department = 'kitchen'
        logger.info(Main_Department)
        logger.info('-----------------')


        logger.info(str(link_number) + ' URL -------- ' + url)
        Created_at = ''
        Updated_at = ''
        start_time = time.strftime('%Y-%m-%d')
        Created_at = str(start_time)
        logger.info(Created_at)
        Updated_at = ''
        start_time = time.strftime('%H-%M')

        repeat_num = 1
        while range(0, 15, 1):
            try:
                sleep(randint(0, 1))
                if start_file >= 1:
                    page = requests.get(url, headers=hdr, proxies=proxyDict)  #
                else:
                    page = requests.get(url, headers=hdr)
                page_status_code = page.status_code
                # logger.info(page_status_code)
            except:
                page_status_code = ''
            if page_status_code == 200:
                logger.info("OK")
                break
            else:
                if page_status_code == 404:
                    logger.error("404 error")
                    break
                else:
                    logger.info("REPEAT " + str(repeat_num))
                    repeat_num += 1
                    if repeat_num == 15:
                        break
                    else:
                        pass
        logger.info("************************ 1")
        page_source = ''
        page_source = page.text
        page_source_string = str(page_source)
        page_source_string = h.unescape(page_source_string)
        additional_page_sorurce = ''
        additional_page_sorurce = page_source_string
        pss = ''
        pss = additional_page_sorurce.replace('\n', '@@@').replace('<div id="about-seller-section',
                                                                   '\n<div id="about-seller-section').replace(
            '<div id="feedback-conten', '\n<div id="feedback-conten').replace('<div id="seller-marketing',
                                                                              '\n<div id="seller-marketing').replace(
            '<span id="about-seller-truncated', '\n<span id="about-seller-truncated').replace(
            '<div class="a-row a-spacing-medium">', '\n<div class="a-row a-spacing-medium">').replace(
            '<span class="expandable-action', '\n<span class="expandable-action')
        # # logger.info(pss)

        # logger.info(additional_page_sorurce)
        Seller_Name = ''
        try:
            Seller_Name = re.search('.*sellerNameDiv.*sellerName">(.*)<\/h1>', additional_page_sorurce).group(1)
            logger.info("Seller_Name          " + Seller_Name)
        except:
            logger.error("PROBLEM  Seller_Name")
            Seller_Name = ''
            continue
        ################################################################################
        all_vertical_addres = ''
        all_vertical_addres_before = ''
        all_vertical_addres_later = ''
        try:
            all_vertical_addres = re.search('(<ul class="a-unordered-list.*vertical.*)', additional_page_sorurce).group(
                1)
            all_vertical_addres_before = all_vertical_addres
            all_vertical_addres_later = all_vertical_addres.replace('<ul>', '\n<ul>').replace('</li>', '</li>\n')
            # logger.info("VERICAL   "+all_vertical_addres_later)
        except:
            logger.error("PROBLEM all_vertical_addres")

            all_vertical_addres = ''
        logger.info("****************************")
        # ################################################################################
        # ################################################################################
        about_truncated = ''
        try:
            about_truncated = re.search('(<span id="about-seller-truncated.*)', additional_page_sorurce).group(1)
            cleanr = re.compile('<.*>')
            about_truncated = re.sub(cleanr, '', about_truncated)
            # logger.info("about_truncated          "+about_truncated)
        except:
            about_truncated = ''
            # logger.error('PROBLEM about_truncated')
        # ################################################################################
        # ################################################################################
        about_expanded = ''
        try:
            about_expanded = re.search('(span id="about-seller-expanded.*)', additional_page_sorurce).group(1)
            cleanr = re.compile('<.*>')
            about_expanded = re.sub(cleanr, '', about_expanded)
            # logger.info("about_expanded          " + about_expanded)
        except:
            about_expanded = ''
            # logger.error('PROBLEM about_expanded')
        # ################################################################################
        # ################################################################################
        Company_Name = ''
        try:
            Company_Name = re.search('Gesch.*ftsname:.*>(.*)<\/span', all_vertical_addres_later).group(1)
            logger.info(Company_Name)
        except:
            try:
                Company_Name = re.search('Business Name:.*>(.*)<\/span', all_vertical_addres_later).group(1)
                logger.info(Company_Name)
            except:
                logger.error("PROBLEM Company_Name")
                Company_Name = ''

        Company_Type = ''
        try:
            Company_Type = re.search('Gesch.*ftsart:.*span>(.*)<\/span', all_vertical_addres_later).group(1)
            # logger.info("Company_Type    "+Company_Type)
        except:
            try:
                Company_Type = re.search('Business Type:.*span>(.*)<\/span', all_vertical_addres_later).group(1)
                # logger.info("Company_Type    "+Company_Type)
            except:
                logger.error("PROBLEM Company_Type")
                Company_Type = ''

        Register_No = ''
        try:
            Register_No = re.search('Handelsregisternummer:<\/span>(.*)<\/span', all_vertical_addres_later).group(1)
            # logger.info("Register_No  " + Register_No)
        except:
            try:
                Register_No = re.search('Trade Register Number:<\/span>(.*)<\/span', all_vertical_addres_later).group(1)
                # logger.info("Register_No  " + Register_No)
            except:
                logger.error("PROBLEM Register_No")
                Register_No = ''

        VAT_ID = ''
        try:
            VAT_ID = re.search('UStID:<\/span>(.*)<\/span', all_vertical_addres_later).group(1)
            # logger.info("VAT_ID  " + VAT_ID)
        except:
            try:
                VAT_ID = re.search('VAT Number:<\/span>(.*)<\/span', all_vertical_addres_later).group(1)
                # logger.info("VAT_ID  " + VAT_ID)
            except:
                logger.error("PROBLEM VAT_ID")
                VAT_ID = ''

        Business_Representative_1 = ''
        Business_Representative_2 = ''
        try:
            Business_Representative_1 = re.search('Unternehmensvertreter:<\/span>(.*),(.*)<\/span',
                                                  all_vertical_addres).group(1)
            # logger.info("Business_Representative_1  " + Business_Representative_1)
            Business_Representative_2 = re.search('Unternehmensvertreter:<\/span>(.*),(.*)<\/span',
                                                  all_vertical_addres_later).group(2)

            # logger.info("Business_Representative_2  " + Business_Representative_2)
        except:
            try:
                Business_Representative_1 = re.search('Unternehmensvertreter:<\/span>(.*)<\/span',
                                                      all_vertical_addres_later).group(1)
                # logger.info("Business_Representative_1  " + Business_Representative_1)
                Business_Representative_2 = ''
                # logger.info("Business_Representative_2  " + Business_Representative_2)
            except:
                try:
                    Business_Representative_1 = re.search('Unternehmensvertreter:<\/span>(.*)<\/span',
                                                          all_vertical_addres_later).group(1)
                    # logger.info("Business_Representative_1  " + Business_Representative_1)
                    Business_Representative_2 = ''
                    # logger.info("Business_Representative_2  " + Business_Representative_2)
                except:
                    try:
                        Business_Representative_1 = re.search('Company representative:<\/span>(.*),(.*)<\/span',
                                                              all_vertical_addres).group(1)
                        # logger.info("Business_Representative_1  " + Business_Representative_1)
                        Business_Representative_2 = re.search('Company representative:<\/span>(.*),(.*)<\/span',
                                                              all_vertical_addres_later).group(2)
                        # logger.info("Business_Represe/ntative_2  " + Business_Representative_2)
                    except:
                        try:
                            Business_Representative_1 = re.search('Company representative:<\/span>(.*)<\/span',
                                                                  all_vertical_addres_later).group(1)
                            # logger.info("Business_Representative_1  " + Business_Representative_1)
                            Business_Representative_2 = ''
                            # logger.info("Business_Representative_2  " + Business_Representative_2)
                        except:
                            try:
                                Business_Representative_1 = re.search('Company representative:<\/span>(.*)<\/span',
                                                                      all_vertical_addres_later).group(1)
                                # logger.info("Business_Representative_1  " + Business_Representative_1)
                                Business_Representative_2 = ''
                                # logger.info("Business_Representative_2  " + Business_Representative_2)
                            except:
                                # logger.error("PROBLEM Business_Representative_1 solo")
                                Business_Representative_1 = ''
                                Business_Representative_2 = ''

        Business_Address = ''
        try:
            Business_Address = re.search('Business Address:(.*)', all_vertical_addres_before).group(1)
            Business_Address = Business_Address.replace('<ul class="a-unordered-list a-nostyle a-vertical">','')\
                .replace('<span class="a-list-item">', '').replace(
                '<li>', '').replace('</span>', '').replace('</li>', '\n').replace('</ul>', '').replace(
                '<span class="a-text-bold">', '')
            # logger.info("Business_Address   " + Business_Address)
        except:
            try:
                Business_Address = re.search('Gesch.*ftsadresse:(.*)', all_vertical_addres_before).group(1)
                Business_Address = Business_Address.replace('<ul class="a-unordered-list a-nostyle a-vertical">',
                                                            '').replace('<span class="a-list-item">', '').replace(
                    '<li>', '').replace('</span>', '').replace('</li>', '\n').replace('</ul>', '').replace(
                    '<span class="a-text-bold">', '')
                # logger.info("Business_Address  0 " + Business_Address)
            except:
                try:
                    Business_Address = re.search('Gesch.*ftsadresse:(.*)', pss).group(1)
                    Business_Address = Business_Address.replace('<ul class="a-unordered-list a-nostyle a-vertical">',
                                                                '').replace('<span class="a-list-item">', '').replace(
                        '<li>', '').replace('</span>', '').replace('</li>', '\n').replace('</ul>', '').replace(
                        '<span class="a-text-bold">', '')
                    # logger.info("Business_Address 1  " + Business_Address)
                except:
                    logger.error("PROBLEM  Business_Address")
                    Business_Address = ''
        ################################################################################
        ################################################################################

        Feedback_30d = ''
        Feedback_90d = ''
        Feedback_365d = ''
        Feedback_all_time = ''
        try:
            all_feedback = re.search('(<tr><td class="a-nowrap.*id="returns-content.*)', page_source).group(1)
            all_feedback = all_feedback.replace('<td', '\n<td').replace('</span>','</span>\n')
            # logger.info(all_feedback)
            all_feedbacks = re.findall('<td class="a-text-right"><span>(.*)<\/span', all_feedback)
            # logger.info(len(all_feedbacks))
            # logger.info(all_feedbacks)
            for one_feedback in all_feedbacks[0:1]:
                # logger.info(one_feedback)
                try:
                    Feedback_30d = one_feedback
                except:
                    Feedback_30d = ''

            for one_feedback in all_feedbacks[1:2]:
                # logger.info(one_feedback)
                try:
                    Feedback_90d = one_feedback
                except:
                    Feedback_90d = ''
            for one_feedback in all_feedbacks[2:3]:
                # logger.info(one_feedback)
                try:
                    Feedback_365d = one_feedback
                except:
                    Feedback_365d = ''
            for one_feedback in all_feedbacks[3:4]:
                # logger.info(one_feedback)
                try:
                    Feedback_all_time = one_feedback
                except:
                    Feedback_all_time = ''
        except:
            all_feedback = ''
            # logger.error("PROBLEM all_feedback")
            Feedback_30d == Feedback_90d == Feedback_365d == Feedback_all_time == "X"

        ################################################################################
        ################################################################################
        Pos_Feedback_30d_percentage = ''
        Pos_Feedback_90d_percentage = ''
        Pos_Feedback_365d_percentage = ''
        Pos_Feedback_All_time_percentage = ''
        logger.info("+++++++++++++++++++++++++++++")

        try:
            full_positive = re.search('(<span class="a-color-success">.*<\/span)', page_source).group(1)
            # logger.info(full_positive)
            full_positive = full_positive.replace('<td', '\n<td').replace('</span>','</span>\n')
            # logger.info(full_positive)
            all_possitive = re.findall('<span class="a-color-success">(.*)<\/span', full_positive)
            # logger.info(len(all_possitive))
            # logger.info(all_possitive)
            for one_positive in all_possitive[0:1]:
                # logger.info(one_positive)
                try:
                    Pos_Feedback_30d_percentage = one_positive
                except:
                    Pos_Feedback_30d_percentage = ''
            for one_positive in all_possitive[1:2]:
                # logger.info(one_positive)
                try:
                    Pos_Feedback_90d_percentage = one_positive
                except:
                    Pos_Feedback_90d_percentage = ''
            for one_positive in all_possitive[2:3]:
                # logger.info(one_positive)
                try:
                    Pos_Feedback_365d_percentage = one_positive
                except:
                    Pos_Feedback_365d_percentage = ''
            for one_positive in all_possitive[3:4]:
                # logger.info(one_positive)
                try:
                    Pos_Feedback_All_time_percentage = one_positive
                except:
                    Pos_Feedback_All_time_percentage = ''
        except:
            all_possitive = ''
            # logger.error("PROBLEM all_possitive")
            Pos_Feedback_30d_percentage == Pos_Feedback_90d_percentage == Pos_Feedback_365d_percentage == Pos_Feedback_All_time_percentage == ''

        ################################################################################
        # ################################################################################
        About_Seller_Description = ''
        try:
            About_Seller_Description = re.search('id="about-seller-text">(.*)', pss).group(1)
            # logger.info("00000000000000000000000000000000")
            # logger.info("About_Seller_Description    1   " + About_Seller_Description)
        except:
            try:
                About_Seller_Description = re.search('id="about-seller-text">(.*)', pss).group(1)
                # logger.info("11111111111111111111111")
                # logger.info("About_Seller_Description    2   "+About_Seller_Description)
            except:
                try:
                    About_Seller_Description = re.search(
                        '<span id="about-seller-expanded" class="expandable-expanded-text">(.*)', pss).group(1)
                    # logger.info("22222222222")
                    About_Seller_Description = About_Seller_Description  #
                    # logger.info("About_Seller_Description   3    " + About_Seller_Description)
                except:
                    About_Seller_Description = ''
                    # logger.error("PROBLEM  About_Seller_Description")

        # logger.info("************************************")
        About_Seller_Description = str(About_Seller_Description).replace('@@@', '\n').replace('>', '>\n').replace('<', '\n<')
        # logger.info("~~~~~~~~~~~~~~~~~~~~")

        About_Seller_Description = About_Seller_Description  #.replace('>', '>\n').replace('<', '\n<')
        cleanr = re.compile('<.*>')
        About_Seller_Description = re.sub(cleanr, '', About_Seller_Description)

        About_Seller_Description = About_Seller_Description.replace('\\n\\n', '\\n')

        all_email_addreses = []
        all_email_rows = []

        try:
            Email_rows = re.findall('(Mail.*)', About_Seller_Description)
            # logger.info(len(all_Email_ID))
            for one_email_row in Email_rows:
                # logger.info("PURE row Mail "+one_email_row)
                one_email_row = one_email_row.replace('\\xa0', "").replace('\\r', '').replace('\\u2013', '').replace(
            '\\x80', '').replace('\\x93', '').replace('\\xc3', '').replace('\\x9c', "").replace('\\xbc', '').replace(
            '\\x9e', '').replace('\\xbc', '').replace('\\xa4', '').replace('\\xc2', '').replace('\\x96', '').replace(
            '\\xe2', '').replace('\\xef', "").replace('\\x9a', '').replace('\\xb6', '').replace('\\xac', '').replace(
            '\\xa7', '').replace('\\xe4', '').replace('\\xf6', "").replace('\\t', '').replace('\\xa8', '').replace(
            '\\xfc', '').replace('\\xc2', '').replace('\\xa0', '').replace(';','').replace('"', '').replace('“', '').replace('\\xa6', '')
                if one_email_row in all_email_rows:
                    # logger.info("dupli row")
                    pass
                else:
                    all_email_rows.append(one_email_row)
        except:
            pass

        try:
            Email_rows = re.findall('(mail.*)', About_Seller_Description)
            # logger.info(len(all_Email_ID))

            for one_email_row in Email_rows:
                # logger.info("PURE row mail "+one_email_row)
                one_email_row = one_email_row.replace('\\xa0', "").replace('\\r', '').replace('\\u2013', '').replace(
            '\\x80', '').replace('\\x93', '').replace('\\xc3', '').replace('\\x9c', "").replace('\\xbc', '').replace(
            '\\x9e', '').replace('\\xbc', '').replace('\\xa4', '').replace('\\xc2', '').replace('\\x96', '').replace(
            '\\xe2', '').replace('\\xef', "").replace('\\x9a', '').replace('\\xb6', '').replace('\\xac', '').replace(
            '\\xa7', '').replace('\\xe4', '').replace('\\xf6', "").replace('\\t', '').replace('\\xa8', '').replace(
            '\\xfc', '').replace('\\xc2', '').replace('\\xa0', '').replace(';','').replace('"', '').replace('“', '').replace('\\xa6', '')
                if one_email_row in all_email_rows:
                    # logger.info("dupli row")
                    pass
                else:
                    all_email_rows.append(one_email_row)
        except:
            pass
        try:
            Email_rows = re.findall('(.*@.*)', About_Seller_Description)
            # logger.info(len(Email_rows))
            for one_email_row in Email_rows:
                # logger.info("PURE row @ "+one_email_row)
                if one_email_row in all_email_rows:
                    # logger.info("dupli row")
                    pass
                else:
                    all_email_rows.append(one_email_row)
        except:
            pass

        try:
            Email_rows = re.findall('(.*\s@\s.*)', About_Seller_Description)
            # logger.info(len(all_Email_ID))
            for one_email_row in Email_rows:
                # logger.info("PURE row \s@\s "+one_email_row)
                one_email_row = one_email_row.replace('\\xa0', "").replace('\\r', '').replace('\\u2013', '').replace(
            '\\x80', '').replace('\\x93', '').replace('\\xc3', '').replace('\\x9c', "").replace('\\xbc', '').replace(
            '\\x9e', '').replace('\\xbc', '').replace('\\xa4', '').replace('\\xc2', '').replace('\\x96', '').replace(
            '\\xe2', '').replace('\\xef', "").replace('\\x9a', '').replace('\\xb6', '').replace('\\xac', '').replace(
            '\\xa7', '').replace('\\xe4', '').replace('\\xf6', "").replace('\\t', '').replace('\\xa8', '').replace(
            '\\xfc', '').replace('\\xc2', '').replace('\\xa0', '').replace(';','').replace('"', '').replace('“', '').replace('\\xa6', '')
                if one_email_row in all_email_rows:
                    # logger.info("dupli row")
                    pass
                else:
                    one_email_row = str(one_email_row).replace(' @ ', '@')
                    # logger.info("PROMENJEN "+ one_email_row)
                    all_email_rows.append(one_email_row)
        except:
            pass

        try:
            Email_rows = re.findall('(.*\(@\).*)', About_Seller_Description)
            # logger.info(len(all_Email_ID))
            for one_email_row in Email_rows:
                # logger.info("PURE row  (@) "+one_email_row)
                one_email_row = one_email_row.replace('\\xa0', "").replace('\\r', '').replace('\\u2013', '').replace(
            '\\x80', '').replace('\\x93', '').replace('\\xc3', '').replace('\\x9c', "").replace('\\xbc', '').replace(
            '\\x9e', '').replace('\\xbc', '').replace('\\xa4', '').replace('\\xc2', '').replace('\\x96', '').replace(
            '\\xe2', '').replace('\\xef', "").replace('\\x9a', '').replace('\\xb6', '').replace('\\xac', '').replace(
            '\\xa7', '').replace('\\xe4', '').replace('\\xf6', "").replace('\\t', '').replace('\\xa8', '').replace(
            '\\xfc', '').replace('\\xc2', '').replace('\\xa0', '').replace(';','').replace('"', '').replace('“', '').replace('\\xa6', '')
                if one_email_row in all_email_rows:
                    # logger.info("dupli row")
                    pass
                else:
                    one_email_row = str(one_email_row).replace(' (@) ', '@').replace("(@)", "@").replace(' @ ', '@')
                    # logger.info("PROMENJEN "+ one_email_row)
                    all_email_rows.append(one_email_row)
        except:
            pass

        try:
            Email_rows = re.findall('(.*\(at\).*)', About_Seller_Description)
            # logger.info(len(all_Email_ID))
            for one_email_row in Email_rows:
                # logger.info("PURE row (at) "+one_email_row)
                one_email_row = one_email_row.replace('\\xa0', "").replace('\\r', '').replace('\\u2013', '').replace(
            '\\x80', '').replace('\\x93', '').replace('\\xc3', '').replace('\\x9c', "").replace('\\xbc', '').replace(
            '\\x9e', '').replace('\\xbc', '').replace('\\xa4', '').replace('\\xc2', '').replace('\\x96', '').replace(
            '\\xe2', '').replace('\\xef', "").replace('\\x9a', '').replace('\\xb6', '').replace('\\xac', '').replace(
            '\\xa7', '').replace('\\xe4', '').replace('\\xf6', "").replace('\\t', '').replace('\\xa8', '').replace(
            '\\xfc', '').replace('\\xc2', '').replace('\\xa0', '').replace(';','').replace('"', '').replace('“', '').replace('\\xa6', '')
                if one_email_row in all_email_rows:
                    # logger.info("dupli row")
                    pass
                else:
                    one_email_row = str(one_email_row).replace(' (at) ', '@').replace('(at)', '@')
                    # logger.info("PROMENJEN "+ one_email_row)
                    all_email_rows.append(one_email_row)
        except:
            pass

        try:
            Email_rows = re.findall('(.*\(AT\).*)', About_Seller_Description)
            # logger.info(len(all_Email_ID))
            for one_email_row in Email_rows:
                # logger.info("PURE row (A/T) "+one_email_row)
                one_email_row = one_email_row.replace('\\xa0', "").replace('\\r', '').replace('\\u2013', '').replace(
            '\\x80', '').replace('\\x93', '').replace('\\xc3', '').replace('\\x9c', "").replace('\\xbc', '').replace(
            '\\x9e', '').replace('\\xbc', '').replace('\\xa4', '').replace('\\xc2', '').replace('\\x96', '').replace(
            '\\xe2', '').replace('\\xef', "").replace('\\x9a', '').replace('\\xb6', '').replace('\\xac', '').replace(
            '\\xa7', '').replace('\\xe4', '').replace('\\xf6', "").replace('\\t', '').replace('\\xa8', '').replace(
            '\\xfc', '').replace('\\xc2', '').replace('\\xa0', '').replace(';','').replace('"', '').replace('“', '').replace('\\xa6', '')
                if one_email_row in all_email_rows:
                    # logger.info("dupli row")
                    pass
                else:
                    one_email_row = str(one_email_row).replace(' (AT) ', '@').replace('(AT)', '@')
                    # logger.info("PROMENJEN "+ one_email_row)
                    all_email_rows.append(one_email_row)
        except:
            pass

        # logger.info(len(all_email_rows))
        for one_row in all_email_rows:
            # logger.info("ONE ROW   "+one_row)
            one_row_string = str(one_row).replace("   ", '\n').replace(":", '\n:\n').replace("+", '\n+').replace(',',
                                                                                                                 '\n,\n').replace(
                '"', '\n"\n').replace(' (@) ', '@').replace(' (@)', '@').replace('(@) ', '@').replace("(@)",
                                                                                                      "@").replace(
                ' @ ', '@').replace(' @', '@').replace('@ ', '@').replace(' (at) ', '@').replace(' (at)', '@').replace(
                '(at)', '@').replace(' (AT) ', '@').replace(' (AT)', '@').replace('(AT)', '@').replace(' [@] ',
                                                                                                       '@').replace(
                "[@]", "@").replace(' {@} ', '@').replace("{@}", "@").replace('(punkt)', '.').replace(' . com',
                                                                                                      '.com\n').replace(
                ' . de', '.de\n').replace('. com', '.com\n').replace('. de', '.de\n').replace('.com', '.com\n').replace(
                '.de', '.de\n').replace('E-Mail-Adresse', 'E-Mail-Adresse\n').replace(')', '\n)\n').replace('(',
                                                                                                            '\n(\n').replace(
                '[', '\n[\n').replace(']', '\n]\n')
            one_row = str(one_row_string).replace(' ', '\n')
            # logger.info("ROW PROM          "+one_row)
            # logger.info("CURRENT LIST "+str(all_email_addreses))

            if '@' in one_row:
                try:
                    Email_ID = re.search('\s(.*\s@\s.*)', one_row).group(1)
                    # logger.info("EMAIL-------------"+str(Email_ID))
                    # logger.info("------------------------------------0")
                except:
                    try:
                        Email_ID = re.search('(.*\s@\s.*)\s', one_row).group(1)
                        # logger.info("EMAIL-------------"+str(Email_ID))
                        # logger.info("------------------------------------01")
                    except:
                        try:
                            Email_ID = re.search('\s(.*\s@\s.*)', one_row).group(1)
                            # logger.info("EMAIL-------------"+str(Email_ID))
                            # logger.info("------------------------------------2")
                        except:
                            try:
                                Email_ID = re.search('(.*)\s(.*@.*)\s(.*)', one_row).group(2)
                                # logger.info("EMAIL-------------"+str(Email_ID))
                                # logger.info("------------------------------------1")
                            except:
                                try:
                                    Email_ID = re.search('(.*\s)(.*@.*)', one_row).group(2)
                                    # logger.info("EMAIL-------------"+str(Email_ID))
                                    # logger.info("------------------------------------3")
                                except:
                                    Email_ID = ''
                                    pass
                try:
                    Email_ID = re.search('(.*)\s', Email_ID).group(1)
                    # logger.info('--------------Email_ID s---------------------')
                except:
                    try:
                        Email_ID = re.search('(.*) ', Email_ID).group(1)
                        # logger.info('--------------Email_ID space---------------------')
                    except:
                        Email_ID = re.search('(.*)', Email_ID).group(1)

                try:
                    Email_ID = re.search('\s(.*)', Email_ID).group(1)
                    # logger.info('--------------S Email_ID ---------------------')
                except:
                    try:
                        Email_ID = re.search(' (.*)', Email_ID).group(1)
                        # logger.info('--------------spcae Email_ID ---------------------')
                    except:
                        Email_ID = re.search('(.*)', Email_ID).group(1)

                try:
                    Email_ID = Email_ID.replace(' ', '').replace('"', '').replace('“', '')
                except:
                    pass

                if Email_ID in all_email_addreses:
                    # logger.info('dupli mail')
                    pass
                else:
                    logger.info("Email_ID 1   " + Email_ID)
                    all_email_addreses.append(Email_ID)

            if '(at)' in one_row:
                try:
                    Email_ID = re.search('(.*(at).*)', one_row).group(1)
                    # logger.info("EMAIL-------------"+str(Email_ID))
                    # logger.info("------------------------------------at1")
                except:
                    try:
                        Email_ID = re.search('(.*\s(at)\s.*)', one_row).group(1)
                        # logger.info("EMAIL-------------"+str(Email_ID))
                        # logger.info("------------------------------------at2")
                    except:
                        try:
                            Email_ID = re.search('(.*(at).*)', one_row).group(1)
                            # logger.info("EMAIL-------------"+str(Email_ID))
                            # logger.info("------------------------------------at3")
                        except:
                            Email_ID = ''
                            pass
                try:
                    Email_ID = Email_ID.replace(' ', '').replace('"', '').replace('“', '')
                except:
                    pass

                if Email_ID in all_email_addreses:
                    # logger.info('dupli mail')
                    pass
                else:
                    # logger.info("Email_ID 2  at " + Email_ID)
                    all_email_addreses.append(Email_ID)

            if '(AT)' in one_row:
                try:
                    Email_ID = re.search('\s(.*(AT).*)\s', one_row).group(1)
                    # logger.info(str(Email_ID))
                    # logger.info("------------------------------------ AT 1")
                except:
                    try:
                        Email_ID = re.search('(.*\s(AT)\s.*)', one_row).group(1)
                        # logger.info("EMAIL-------------"+str(Email_ID))
                        # logger.info("------------------------------------ AT 2")
                    except:
                        try:
                            Email_ID = re.search('(.*(AT).*)', one_row).group(1)
                            # logger.info("EMAIL-------------"+str(Email_ID))
                            # logger.info("------------------------------------ AT 3")
                        except:
                            Email_ID = ''
                            pass
                try:
                    Email_ID = Email_ID.replace(' ', '').replace('"', '').replace('“', '')
                except:
                    pass
                # Email_ID = Email_ID.replace('\s', '')
                if Email_ID in all_email_addreses:
                    # logger.info('dupli mail')
                    pass
                else:
                    logger.info("Email_ID  3  " + Email_ID)
                    all_email_addreses.append(Email_ID)

        # logger.info(" EMAILS           ")
        # logger.info(all_email_addreses)
        # logger.info(" *****************************")
        string_all_emais = str(all_email_addreses).replace("', u'", "; ").replace("[u'", '').replace("[", "").replace(
            "]", "").replace(",", "; ").replace("'", "")
        # logger.info("EMAILS         " + str(string_all_emais))
        string_all_emais = str(string_all_emais).replace('\\xa0', "").replace('\\r', '').replace('\\u2013', '').replace(
            '\\x80', '').replace('\\x93', '').replace('\\xc3', '').replace('\\x9c', "").replace('\\xbc', '').replace(
            '\\x9e', '').replace('\\xbc', '').replace('\\xa4', '').replace('\\xc2', '').replace('\\x96', '').replace(
            '\\xe2', '').replace('\\xef', "").replace('\\x9a', '').replace('\\xb6', '').replace('\\xac', '').replace(
            '\\xa7', '').replace('\\xe4', '').replace('\\xf6', "").replace('\\t', '').replace('\\xa8', '').replace(
            '\\xfc', '').replace('\\xc2', '').replace('\\xa0', '').replace('“', '').replace('\\xa6', '')

        logger.info("EMAILS     -----------------------------------------------  utf-8     " + string_all_emais)
        # logger.info(" *****************************")
        # logger.info("ROWS    "   +str(all_email_rows))
        # ################################################################################
        # ################################################################################
        Service_Address = ''
        try:
            Service_Address = re.search('Customer Services Address:(.*)Business Address:',
                                        all_vertical_addres_before).group(1)
            Service_Address = Service_Address.replace('<ul class="a-unordered-list a-nostyle a-vertical">',
                                                      '').replace('<span class="a-list-item">', '').replace('<li>',
                                                                                                            '').replace(
                '</span>', '').replace('</li>', '\n').replace('</ul>', '').replace('<span class="a-text-bold">', '')
            # logger.info("Service_Address  " + Service_Address)
        except:
            try:
                Service_Address = re.search('Kundendienstadresse:(.*)Gesch',
                                            all_vertical_addres_before).group(1)
                Service_Address = Service_Address.replace('<ul class="a-unordered-list a-nostyle a-vertical">',
                                                          '').replace('<span class="a-list-item">', '').replace(
                    '<li>',
                    '').replace(
                    '</span>', '').replace('</li>', '\n').replace('</ul>', '').replace('<span class="a-text-bold">',
                                                                                       '')
                # logger.info("Service_Address  DE  " + Service_Address)
            except:
                # logger.info("NO SERVIS ADDRESS")
                Service_Address = ''

        Store_URL = url
        main_Contact_Number = ''
        all_contact_numbers = []
        all_phone_rows = []
        contact_control = []
        all_fax_numbers = []
        try:
            Contact_Number_all = re.findall('Telefon.*>(.*)<.span', all_vertical_addres_later)
            # logger.info(len(Contact_Number_all))
            for main_Contact_Number in Contact_Number_all:
                try:
                    contact = main_Contact_Number.replace(' ', '').replace('+', '').replace('(', '').replace(')',
                                                                                                             '').replace(
                        '-', '').replace('/', '')
                    # logger.info("only digits phone main  :    " + contact)
                except:
                    logger.info('no phone main replace')
                    contact = main_Contact_Number
                logger.info(len(contact))
                if len(contact) >= 7:
                    if contact in contact_control or main_Contact_Number in all_contact_numbers:
                        # logger.info('duplicate')
                        pass
                    else:
                        contact_control.append(contact)
                        # logger.info("PHONE MAIN " + main_Contact_Number)
                else:
                    # logger.info("NO PHONE MAIN  " + str(main_Contact_Number))
                    pass
        except:
            # logger.error("PROBLEM  Contact_Number")
            Contact_Number = ''
        # logger.info("*******************************************************")

        try:
            Contact_Number_all = re.findall('contact-phone.*>(.*)<\/span', additional_page_sorurce)
            # logger.info(len(Contact_Number_all))
            for Contact_Number in Contact_Number_all:
                # logger.info(Contact_Number)
                try:
                    contact = Contact_Number.replace(' ', '').replace('+', '').replace('(', '').replace(')',
                                                                                                        '').replace('-',
                                                                                                                    '').replace(
                        '/', '')
                    # logger.info("only CONAC   :    " + contact)
                except:
                    # logger.info('nije')
                    contact = Contact_Number
                # logger.info(len(contact))
                if len(contact) >= 7:
                    if contact in contact_control or Contact_Number in all_contact_numbers:
                        # logger.info('duplicate')
                        pass
                    else:
                        contact_control.append(contact)
                        # logger.info("PHONE additional " + Contact_Number)
                        all_contact_numbers.append(Contact_Number)
                else:
                    # logger.info("NIJE BROJ TELEFONA  " + str(Contact_Number))
                    pass
        except:
            # logger.error("PROBLEM  Contact_Number")
            Contact_Number = ''
        # logger.info("*******************************************************")

        try:
            phone_rows = re.findall('(Tel.*\d{3}.*)', About_Seller_Description)
            for one_phone in phone_rows:
                # logger.info(one_phone)
                try:
                    more_than_3 = re.search('(\d{3})', one_phone).group(1)
                except:
                    # logger.info("WRONG ROW")
                    continue
                if one_phone in all_phone_rows:
                    # logger.info("dupli phone")
                    pass
                else:
                    # logger.info("Phone 1  " + one_phone)
                    all_phone_rows.append(one_phone)
        except:
            pass
        #
        # logger.info(all_vertical_addres_later)
        # logger.info("********************")

        try:
            phone_rows = re.findall('(Tel.*\d{3}.*)', all_vertical_addres_later)
            for one_phone in phone_rows:
                # logger.info("PRE"+one_phone)
                try:
                    more_than_3 = re.search('(\d{3})', one_phone).group(1)
                except:
                    # logger.info("WRONG ROW")
                    continue
                one_phone = one_phone.replace('</span>', '').replace('</li>', '')
                if one_phone in all_phone_rows:
                    # logger.info("dupli phone row")
                    pass
                else:
                    # logger.info("all_vertival_address "+one_phone)
                    all_phone_rows.append(one_phone)
        except:
            pass

        # logger.info("************************")
        # logger.info(all_phone_rows)
        # logger.info("************************")

        for one_row in all_phone_rows:
            # logger.info("ONE phone row      " + one_row)
            one_row = one_row.decode('utf-8')
            # logger.info(one_row)
            one_row = one_row.replace("Tel", "\nTel").replace('E-Mail', '\nE-Mail').replace('Mail', '\nMail').replace(
                'Fax', '\nFax')
            # logger.info(one_row)
            one_row = str(one_row).replace(';', '\n;\n').replace("   ", '\n').replace(":", ':\n').replace("+",
                                                                                                          '\n+').replace(
                ',', '\n,\n').replace('"', '"\n').replace(" A", "\nA").replace(" a", "\na").replace(" B",
                                                                                                    "\nB").replace(" b",
                                                                                                                   "\nb").replace(
                " C", "\nC").replace(" c", "\nc").replace(" D", "\nD").replace(" d", "\nd").replace(" E",
                                                                                                    "\nE").replace(" e",
                                                                                                                   "\ne").replace(
                " F", "\nF").replace(" f", "\nf").replace(" G", "\nG").replace(" g", "\ng").replace(" H",
                                                                                                    "\nH").replace(" h",
                                                                                                                   "\nh").replace(
                " I", "\nI").replace(" i", "\ni").replace(" J", "\nJ").replace(" j", "\nj").replace(" K",
                                                                                                    "\nK").replace(" k",
                                                                                                                   "\nk").replace(
                " L", "\nL").replace(" l", "\nl").replace(" M", "\nM").replace(" m", "\nm").replace(" N",
                                                                                                    "\nN").replace(" n",
                                                                                                                   "\nn").replace(
                " O", "\nO").replace(" o", "\no").replace(" P", "\nP").replace(" p", "\np").replace(" Q",
                                                                                                    "\nQ").replace(" q",
                                                                                                                   "\nq").replace(
                " R", "\nR").replace(" r", "\nr").replace(" S", "\nS").replace(" s", "\ns").replace(" T",
                                                                                                    "\nT").replace(" t",
                                                                                                                   "\nt").replace(
                " U", "\nU").replace(" u", "\nu").replace(" V", "\nV").replace(" v", "\nv").replace(" W",
                                                                                                    "\nW").replace(" w",
                                                                                                                   "\nw").replace(
                " X", "\nX").replace(" x", "\nx").replace(" Y", "\nY").replace(" y", "\ny").replace(" Z",
                                                                                                    "\nZ").replace(" z",
                                                                                                                   "\nz").replace(
                "A ", "A\n").replace("a ", "a\n").replace("B ", "B\n").replace("b ", "b\n").replace("C ",
                                                                                                    "C\n").replace("c ",
                                                                                                                   "c\n").replace(
                "D ", "D\n").replace("d ", "d\n").replace("E ", "E\n").replace("e ", "e\n").replace("F ",
                                                                                                    "F\n").replace("f ",
                                                                                                                   "f\n").replace(
                "G ", "G\n").replace("g ", "g\n").replace("H ", "H\n").replace("h ", "h\n").replace("I ",
                                                                                                    "I\n").replace("i ",
                                                                                                                   "i\n").replace(
                "J ", "J\n").replace("j ", "j\n").replace("K ", "K\n").replace("k ", "k\n").replace("L ",
                                                                                                    "L\n").replace("l ",
                                                                                                                   "l\n").replace(
                "M ", "M\n").replace("m ", "m\n").replace("N ", "N\n").replace("n ", "n\n").replace("O ",
                                                                                                    "O\n").replace("o ",
                                                                                                                   "o\n").replace(
                "P ", "P\n").replace("p ", "p\n").replace("Q ", "Q\n").replace("q ", "q\n").replace("R ",
                                                                                                    "R\n").replace("r ",
                                                                                                                   "r\n").replace(
                "S ", "S\n").replace("s ", "s\n").replace("T ", "T\n").replace("t ", "t\n").replace("U ",
                                                                                                    "U\n").replace("u ",
                                                                                                                   "u\n").replace(
                "V ", "V\n").replace("v ", "v\n").replace("W ", "W\n").replace("w ", "w\n").replace("X ",
                                                                                                    "X\n").replace("x ",
                                                                                                                   "x\n").replace(
                "Y ", "Y\n").replace("y ", "y\n").replace("Z ", "Z\n").replace("z ", "z\n").replace("(A",
                                                                                                    "\n(A").replace(
                "(a", "\n(a").replace("(B", "\n(B").replace("(b", "\n(b").replace("(C", "\n(C").replace("(c",
                                                                                                        "\n(c").replace(
                "(D", "\n(D").replace("(d", "\n(d").replace("(E", "\n(E").replace("(e", "\n(e").replace("(F",
                                                                                                        "\n(F").replace(
                "(f", "\n(f").replace("(G", "\n(G").replace("(g", "\n(g").replace("(H", "\n(H").replace("(h",
                                                                                                        "\n(h").replace(
                "(I", "\n(I").replace("(i", "\n(i").replace("(J", "\n(J").replace("(j", "\n(j").replace("(K",
                                                                                                        "\n(K").replace(
                "(k", "\n(k").replace("(L", "\n(L").replace("(l", "\n(l").replace("(M", "\n(M").replace("(m",
                                                                                                        "\n(m").replace(
                "(N", "\n(N").replace("(n", "\n(n").replace("(O", "\n(O").replace("(o", "\n(o").replace("(P",
                                                                                                        "\n(P").replace(
                "(p", "\n(p").replace("(Q", "\n(Q").replace("(q", "\n(q").replace("(R", "\n(R").replace("(r",
                                                                                                        "\n(r").replace(
                "(S", "\n(S").replace("(s", "\n(s").replace("(T", "\n(T").replace("(t", "\n(t").replace("(U",
                                                                                                        "\n(U").replace(
                "(u", "\n(u").replace("(V", "\n(V").replace("(v", "\n(v").replace("(W", "\n(W").replace("(w",
                                                                                                        "\n(w").replace(
                "(X", "\n(X").replace("(x", "\n(x").replace("(Y", "\n(Y").replace("(y", "\n(y").replace("(Z",
                                                                                                        "\n(Z").replace(
                "(z",
                "\n(z")
            # logger.info("after         " + one_row)
            if 'fax' in one_row or 'Fax' in one_row:
                # logger.info("FAX row-----------------" + one_row)
                try:
                    Fax_broj = re.search('(..*\d{3}.*)', one_row).group(1)
                    # logger.info("FAX Telefax .*:      "+Fax_broj)
                except:
                    try:
                        Fax_broj = re.search('(..*\d{3}.*)', one_row).group(1)
                        # logger.info("FAX Telefax \s    "+Fax_broj)
                    except:
                        try:
                            Fax_broj = re.search('(..*\d{3}.*)', one_row).group(1)
                            # logger.info("FAX Telefax \s    "+Fax_broj)
                        except:
                            logger.error("-------------------------------------------------ROBLEM FAX")
                            Fax_broj = ''
                try:
                    Fax_broj = re.search('(.*\d)', Fax_broj).group(1)
                    # logger.info('--------------Fax_broj\s---------------------')
                except:
                    Fax_broj = re.search('(.*\d{3}.*)', Fax_broj).group(1)
                try:
                    Fax_broj = re.search('(\+.*)', Fax_broj).group(1)
                    # logger.info('--------------+Fax_broj---------------------')
                except:
                    try:
                        Fax_broj = re.search('(\d.*)', Fax_broj).group(1)
                        # logger.info('--------------\sFax_broj---------------------')
                    except:
                        Fax_broj = re.search('(.*\d{3}.*)', Fax_broj).group(1)

                try:
                    fax_contac = Fax_broj.replace(' ', '').replace('+', '').replace('(', '').replace(')', '').replace(
                        '-', '').replace('/', '')
                    # logger.info(fax_contac)
                except:
                    # logger.info('nije')
                    fax_contac = Fax_broj
                # logger.info(len(fax_contac))
                if len(fax_contac) >= 7:
                    if fax_contac in contact_control or Fax_broj in all_contact_numbers:
                        # logger.info('duplicate')
                        pass
                    else:
                        contact_control.append(fax_contac)
                        # logger.info("PHONE FAX " + Fax_broj)
                        all_fax_numbers.append(Fax_broj)
                else:
                    # logger.info("NIJE FAX BROJ TELEFONA  " + str(Fax_broj))
                    pass
            else:
                vise_Contact_Number = re.findall('(.*\d{3}.*)', one_row)  #.group(1)
                # logger.info(len(vise_Contact_Number))
                for Contact_Number in vise_Contact_Number:
                    Contact_Number = Contact_Number.replace('Telefonnummer:', 'Telefonnummer:\n').replace('Tel.',
                                                                                                          'Tel.\n').replace(
                        'Telefon:',
                        'Telefon:\n')  #.replace('Telefonnummer: ','Telefonnummer: \n').replace('Tel. ','Tel. \n').replace('Telefon ', 'Telefon \n')
                    # logger.info(contact_control)
                    # logger.info('-----')
                    # logger.info(all_con_numbers)
                    Contact_Number = str(Contact_Number).replace(' (0) ', '(0)').replace('   ', ' ').replace(' (0)',
                                                                                                             '(0)').replace(
                        '(0) ', '(0)').replace(' / ', '/')
                    Contact_Number = str(Contact_Number).replace(' - ', '-').replace(' -', '-').replace('- ', '-')
                    # logger.info("Contact_Number  short        " + Contact_Number)
                    try:
                        Contact_Number = re.search('(.*\d)', Contact_Number).group(1)
                        # logger.info('--------------contact s---------------------')
                    except:
                        Contact_Number = re.search('(.*\d{3}.*)', Contact_Number).group(1)
                    try:
                        Contact_Number = re.search('(\+.*)', Contact_Number).group(1)
                        # logger.info('--------------+---------------------')
                    except:
                        try:
                            Contact_Number = re.search('(\d.*)', Contact_Number).group(1)
                            # logger.info('--------------S contact ---------------------')
                        except:
                            Contact_Number = re.search('(.*\d{3}.*)', Contact_Number).group(1)
                    # logger.info("NUMBER   " + Contact_Number)
                    try:
                        contact = Contact_Number.replace(' ', '').replace('+', '').replace('(', '').replace(')',
                                                                                                            '').replace(
                            '-', '').replace('/', '')
                        # logger.info("only digits   :    " + contact)
                    except:
                        # logger.info('NO')
                        contact = Contact_Number
                    # logger.info(len(contact))
                    if len(contact) >= 7:
                        if contact in contact_control or Contact_Number in all_contact_numbers:
                            # logger.info('duplicate')
                            pass
                        else:
                            contact_control.append(contact)
                            # logger.info("PHONE" + str(Contact_Number))

                            all_contact_numbers.append(Contact_Number)
                    else:
                        # logger.info("ITS NOT PHONE NUMBER  " + str(Contact_Number))
                        pass
                        # logger.info("************************")
                        ###############################################################################
                        # logger.info("all_contact_numbers **************" + str(all_contact_numbers))
                        # logger.info(all_phone_rows)

        all_fax_numbers = str(all_fax_numbers).replace("', u'", ";").replace("', '", ";").replace("u'", '').replace("'",
                                                                                                                    "")
        # logger.info("all_fax_numbers            " + all_fax_numbers)
        all_fax_numbers = str(all_fax_numbers).replace('\\xe2\\x80\\x93', '-').replace('[','').replace(']', '').replace('\\xc2\\xa0', '').replace('\\xe2\\x80\\xad', ' ')
        logger.info("all_fax_numbers   ------------------------------------    utf-8     " + all_fax_numbers)

        # logger.info("-----------------------")
        # logger.info(len(all_contact_numbers))
        # logger.info(len(all_phone_rows))
        str_all_contact_numbers = str(all_contact_numbers)
        final_all_contact_numbers = str(str_all_contact_numbers).replace("', u'", ";").replace("', '", ";").replace(
            "u'", '').replace("'", "")
        # logger.info("final_all_contact_numbers            " + str(final_all_contact_numbers))
        final_all_contact_numbers = str(final_all_contact_numbers).replace('\\xe2\\x80\\x93', '-').replace('[','').replace(']', '').replace('\\xc2\\xa0', '').replace('\\xe2\\x80\\xad', ' ')
        logger.info(
            "final_all_contact_numbers   ------------------------------------    utf-8     " + final_all_contact_numbers)
        control_contact_number = len(all_contact_numbers)

        PHONE = str(final_all_contact_numbers)
        Fax = str(all_fax_numbers)
        part_of_url = re.search('seller=(.*)&tab', url).group(1)
        ID = part_of_url
        if control_contact_number >= 1 and final_all_contact_numbers == []:
            logger.error(
                "--------------------------------------------------------------------------------------MORE ROWS")
        Sub_Department = ''
        logger.info('in data')
        podaci = (str(Marketplace), str(Main_Department), str(Sub_Department), str(Seller_Name), str(Company_Name),
                  str(Business_Address), str(Service_Address), str(Company_Type), str(Register_No), str(VAT_ID),
                  str(Business_Representative_1), str(Business_Representative_2), str(string_all_emais),
                  str(main_Contact_Number), final_all_contact_numbers, all_fax_numbers, str(Feedback_30d),
                  str(Feedback_90d), str(Feedback_365d), str(Feedback_all_time), str(Pos_Feedback_30d_percentage),
                  str(Pos_Feedback_90d_percentage), str(Pos_Feedback_365d_percentage),
                  str(Pos_Feedback_All_time_percentage), str(About_Seller_Description), Store_URL, Created_at,
                  Updated_at)
        logger.info(podaci)
        if '<' in str(podaci):
            logger.info("wrong")
            pass
        else:
            mycursor.execute(sqlFormula, podaci)
            time.sleep(1)
            logger.info('-------------------')
            mydb.commit()

            info_writer2.writerow([url])
            control_list.append(url)
            logger.info("-------------write in MySQL----------------")

        try:
            Marketplace == Main_Department == Sub_Department == Seller_Name == Company_Name == Business_Address == Service_Address == Company_Type == Register_No == VAT_ID == Business_Representative_1 == Business_Representative_2 == string_all_emais == final_all_contact_numbers == Feedback_30d == Feedback_90d == Feedback_365d == Feedback_all_time == Pos_Feedback_30d_percentage == Pos_Feedback_90d_percentage == Pos_Feedback_365d_percentage == Pos_Feedback_All_time_percentage == About_Seller_Description == Store_URL == all_phone_rows == all_email_rows == Created_at == Updated_at == ''
            logger.info("******************/***** clear *************************")
        except:
            logger.error("something wrong with clear")



