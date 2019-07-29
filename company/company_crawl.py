import sys, csv, logging, datetime
import requests
from bs4 import BeautifulSoup
from urllib.request import Request, urlopen
from processor import readFile, writeFile, converterHtml, getConnection, getLogging, writerErrorFile
from constant import CITY
import mysql.connector
from mysql.connector import Error, errors
from mysql.connector import errorcode


url = 'http://www.thongtincongty.com/'
soup = converterHtml(url)
list_url = soup.find('div', class_='list-group').find_all('a')
logger = getLogging()
logger.info('Run cawldata company')
# take page, href, city_index in file
if readFile():
    page_in_file = readFile()[0][1]
    url_in_file = readFile()[0][0]
    for i in range(0,64):
        if list_url[i].get('href') == url_in_file: 
            href_cuont = i 
            break
        else : href_cuont = 0
else:
    url_in_file = ''
    href_cuont = 0
try:
    connection = getConnection()
    if (connection.is_connected()):
        db_Info = connection.get_server_info()
        logger.info('Connected to MySQL database... MySQL Server version on '+db_Info)
    cursor = connection.cursor()
    for tag_a in list_url[href_cuont:64] :
        city = tag_a.get_text()
        # transfer city to city index
        for row_city in CITY :
            if city == row_city[1] : 
                city_index = row_city[0]
                break
            else :
                city_index = None 
        # check href
        if url_in_file == tag_a.get('href') :
            pageWeb = int(page_in_file) + 1
        else:
            pageWeb = 1
        pageCount = pageWeb
        while pageCount > 0:
            url_city = tag_a.get('href') +'?page=%d'%(pageCount)
            soup = converterHtml(url_city)
            pageCount = pageCount + 1
            datetime_now = datetime.datetime.now()
            try:
                list_div = soup.find('div', class_='col-xs-12 col-sm-9').find_all('div', class_='search-results')
                writeFile(tag_a.get('href'), pageCount)
                for tag_div in list_div:
                    name = tag_div.find('a').get_text()
                    address = tag_div.find('p').get_text().split('Địa chỉ:')[1]
                    try:
                        # insert data
                        sql ="""insert into job_company(name,is_ot, nation, is_outsourcing, 
                                is_product,is_fake, is_active, created, updated) 
                                values (%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
                        cursor.execute(sql,(name,0,'VN',0,0,0,1,datetime_now,datetime_now))
                        connection.commit()
                        sql_branch = """insert into job_companybranch(city, address, company_id) 
                                        values (%s, %s, %s)"""
                        cursor.execute(sql_branch,(city_index, address, cursor.lastrowid))
                        connection.commit()               
                    except mysql.connector.Error as error :
                        try :
                            # query = "select id from job_company where name=%s"
                            # result  = cursor.execute(query,(name,))
                            # id_branch = cursor.fetchone()[0]                            
                            # sql_branch = """insert into job_companybranch(city, address, company_id) 
                            #                 values(%s,%s,%s)"""
                            sql_branch = """insert into job_companybranch(city, address, company_id) 
                                            select %s,%s,id from job_company where name=%s"""
                            cursor.execute(sql_branch,(city_index, address, name,))
                            connection.commit()
                            print(name)
                        except Error as error :
                            connection.rollback()
                            logger.warning('error unique_together :'+url_city)
                            logger.error(error)
            except Exception as e:
                writerErrorFile(url_city)
                logger.error(str(e))
                break

except Error as e:
    logger.error('no connection :'+str(e))
finally:
    if (connection.is_connected()):
        cursor.close()
        connection.close()
    logger.info('close the connection database')


    