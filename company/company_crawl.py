# -*- coding: utf-8 -*-
import sys, csv, logging, datetime, re, requests
import requests
from bs4 import BeautifulSoup
from urllib.request import Request, urlopen,HTTPError
from processor import (
    readFile, writeFile, converterHtml, getConnection, getLogging, writerErrorFile,
    joinSquence, writerPageError, gethtml
)
from constant import CITY
import mysql.connector
from mysql.connector import Error, errors
from mysql.connector import errorcode

url = 'https://congtydoanhnghiep.com/'
content =requests.get(url)
soup = BeautifulSoup(content.text, 'html.parser')
list_city = soup.find('ul', class_='list-group').findAll('li',class_='list-group-item')
logger = getLogging()
logger.info('========RUN CRAWL DATA=========')
regex = r'^[0-9\(\)\ /\+\-]*$'
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
        logger.info('Connected to MySQL database')
    cursor = connection.cursor()

    # data city
    for tag_a in list_city[city_cuont:len(list_city)] :
        city = tag_a.find('a').get_text()
        logger.info('1 :'+city)
        # transfer city to city index
        for row_city in CITY :
            if city == row_city[1] : 
                city_index = row_city[0]
                break
            else :
                city_index = None 

        # Get the county data of the city 
        city_href = tag_a.find('a').get('href')
        content =requests.get(city_href)
        soup_county = BeautifulSoup(content.text, 'html.parser')
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
            logger.info('2 :contry_href :'+county_href)
            # check href county
            if county_url_file == county.find('a').get('href') :
                pageWeb = int(page_county_file) 
            else:
                pageWeb = 1
            pageCount = pageWeb
            while pageCount > 0:
                url_county = county_href +'/trang-%d'%(pageCount)
                # logger.info('3 :'+url_county)
                try:
                    # soup = converterHtml(url_county)
                    content = gethtml(url_county)
                    soup = BeautifulSoup(content.text, 'html.parser')
                    pageCount = pageCount + 1
                    list_div = soup.find('div', class_='col-sm-8 table-striped').find_all('article')
                    writeFile(city_href,county_href,pageCount)
                except requests.exceptions.Timeout as error:
                    writerPageError(url_county)
                    logger.error('4 :'+str(error)+'-url'+url_county)
                    continue         
                # except HTTPError as error:
                #     logger.info('5 :'+str(error))     
                #     break                                 
                except Exception as error:
                    if content.status_code ==502:
                        logger.info('502 Bad Gateway')
                        break
                    logger.error('6 :'+str(error))
                    continue
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
                            phone_regex = information[1].find_all('td')[1].text
                            phone_number = re.search(regex,phone_regex).group()
                        except :
                            phone_number = None
                    except Exception as error:
                        writerErrorFile(url,county_href,pageCount-1)
                        logger.error('7 :'+str(error)+'-url :'+url)
                        continue
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
                    except errors.IntegrityError as error:        
                        try :        
                            sql_branch = """insert into companies_companybranch(city, address, crawl_url, company_id)         
                                            select %s ,%s ,%s ,id from job_company where name = %s"""        
                            cursor.execute(sql_branch,(city_index, address, url, name_company,))        
                            connection.commit()        
                        except Error as error :        
                            connection.rollback()        
                            writerErrorFile(url,county_href,pageCount-1)        
                            logger.error('8 :'+str(error)+'-'+url)        
                    except Exception as error :        
                        writerErrorFile(url,county_href,pageCount-1)                                        
                        logger.error('9 :'+str(error)+'- :'+url)         
except Error as error:  
    logger.error('10 :no connection :'+str(error))
except Exception as error:
    logger.error('11 :'+str(error))
finally:
    if(connection.is_connected()):
        cursor.close()
        connection.close()
    logger.info('close the connection database\n')
