import sys, csv, logging, datetime
import requests
from bs4 import BeautifulSoup
from urllib.request import Request, urlopen
from processor import (
    readFile, writeFile, converterHtml, getConnection, getLogging, writerErrorFile, joinSquence
)
from constant import CITY
import mysql.connector
from mysql.connector import Error, errors
from mysql.connector import errorcode

url = 'https://congtydoanhnghiep.com/'
soup = converterHtml(url)
list_city = soup.find('ul', class_='list-group').find_all('li',class_='list-group-item')
logger = getLogging()
logger.info('========RUN CRAWL DATA=========')
# take page, href, city_index in file
if readFile():
    city_url_file = readFile()[0][0]
    county_url_file = readFile()[0][1]
    page_county_file = readFile()[0][2]
    for i in range(0,len(list_city)):
        if list_city[i].find('a').get('href') == city_url_file: 
            city_cuont = i 
            break
        else : city_cuont = 0
else:
    city_url_file = ''
    county_url_file = ''
    city_cuont = 0
try:
    connection = getConnection()
    if (connection.is_connected()):
        db_Info = connection.get_server_info()
        logger.info('Connected to MySQL database... MySQL Server version on '+db_Info)
    cursor = connection.cursor()

    # data city
    for tag_a in list_city[city_cuont:len(list_city)] :
        city = tag_a.find('a').get_text()

        # transfer city to city index
        for row_city in CITY :
            if city == row_city[1] : 
                city_index = row_city[0]
                break
            else :
                city_index = None 

        # Get the county data of the city 
        city_href = tag_a.find('a').get('href')
        soup_county = converterHtml(city_href)
        list_county = soup_county.find('ul', class_='list-group').find_all('li',class_='list-group-item')

        # check in which district
        for i in range(0, len(list_county)):
            if county_url_file == list_county[i].find('a').get('href'):
                county_cuont = i
                break
            else:
                county_cuont = 0
        for county in list_county[county_cuont:len(list_county)] :
            county_href = county.find('a').get('href')
            logger.info('contry_href :'+county_href)
            # check href county
            if county_url_file == county.find('a').get('href') :
                pageWeb = int(page_county_file) 
            else:
                pageWeb = 1
            pageCount = pageWeb
            while pageCount > 0:
                url_county = county_href +'/trang-%d'%(pageCount)
                try:
                    soup = converterHtml(url_county)
                    pageCount = pageCount + 1
                    list_div = soup.find('div', class_='col-sm-8 table-striped').find_all('article')
                    writeFile(city_href,county_href,pageCount)
                    for tag_div in list_div:
                        try:
                            url = tag_div.find('a').get('href')
                            soup_url = converterHtml(url)
                            information = soup_url.find('div', class_='row').find_all('table', class_='table last-left')
                            name_company = soup_url.find('td').get_text()
                            representative = information[0].text.split('Chủ sở hữu:')[1].split('\n')[0]
                            tax_code = information[0].text.split('Mã số thuế:')[1].split('\n')[0].strip()
                            address = information[1].find('td').get_text()
                            status = information[2].find('td').text
                            operate = information[2].find_all('td')[2].text
                            date_operate = joinSquence(operate)
                            try:
                                company_type = information[3].text.split('Ngành nghề chính:')[1].split('Ngành nghề kinh doanh:')[0].replace('\n', '')
                            except :
                                company_type = None                          
                            try:
                                phone_number = information[1].find_all('td')[1].text
                            except :
                                phone_number = None
                            try:
                            # insert data
                                sql ="""insert into company(name, tax_code, company_type,
                                        representative, date_operate, status, phone_number) 
                                        values (%s,%s,%s,%s,%s,%s,%s)"""
                                insert_tuple  = (name_company, tax_code, company_type,representative, date_operate, status, phone_number)
                                cursor.execute(sql, insert_tuple)
                                connection.commit()
                                sql_branch = """insert into companybranch(city, address, crawl_url, company_id) 
                                                values (%s, %s, %s, %s)"""
                                cursor.execute(sql_branch,(city_index, address, url, cursor.lastrowid))
                                connection.commit()                           
                            except Exception as error :
                                writerErrorFile(url,county_href,pageCount)                                
                                logger.error('error :'+str(error)+'url :'+url)
                                try :
                                    sql_branch = """insert into companies_companybranch(city, address, crawl_url, company_id) 
                                                    select %s ,%s ,%s ,id from job_company where name = %s"""
                                    cursor.execute(sql_branch,(city_index, address, url, name_company,))
                                    connection.commit()
                                except Error as error :
                                    connection.rollback()
                                    logger.warning('unique_together :'+url)
                                    logger.error(error)
                        except Exception as error:
                            writerErrorFile(url,county_href,pageCount)
                            logger.error('url :'+url+' error : '+str(error))
                except TimeoutError as er:
                    logger.error(str(er)+'-url'+url_county)
                    continue                                               
                except Exception as e:
                    logger.error(str(e))
                    break
except Error as e:
    logger.error('no connection :'+str(e))
except Exception as error:
    logger.error(str(error))
finally:
    if(connection.is_connected()):
        cursor.close()
        connection.close()
    logger.info('close the connection database\n')

