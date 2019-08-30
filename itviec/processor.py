# -*- coding: utf-8 -*-
import sys, csv, logging, requests
from datetime import datetime
import mysql.connector

def getLogging():
    LOG_FORMAT = "%(asctime)s [%(levelname)s]- %(message)s"
    logging.basicConfig(handlers=[logging.FileHandler('access_log.log', 'a', 'utf-8')],
                        level = logging.DEBUG,
                        format = LOG_FORMAT)
    logger = logging.getLogger()
    return logger

def distanceTime(str_time):
    job_posted = str_time.split(' ',1)
    time_now = datetime.now()
    if job_posted[1] == 'minutes ago' or job_posted[1] == 'minute ago':
       timestamp = datetime.timestamp(time_now) - (int(job_posted[0])*60) 
       return timestamp
    elif job_posted[1] == 'hours ago' or job_posted[1] == 'hour ago':
        timestamp = datetime.timestamp(time_now) - (int(job_posted[0])*3600)
        return timestamp
    elif job_posted[1] == 'days ago' or job_posted[1] == 'day ago':
        timestamp = datetime.timestamp(time_now) - (int(job_posted[0])*86400)
        return timestamp
    else:
        timestamp = datetime.timestamp(time_now)
        return timestamp

def getConnection():
    connection = mysql.connector.connect(host="localhost",
                                         port=3306, 
                                         user="root", 
                                         db="job_company",
                                         password="")
                                        #  charset="utf8mb4" ,
                                        #  COLLATE= "utf8mb4_unicode_ci")
    return connection

def writeFile(job_href, pageCount):
    f = csv.writer(open('page_url.csv', 'w'))
    f.writerow([job_href, pageCount])

def writerPageError(url_county):
    f = csv.writer(open('page_error.csv', 'a', newline=''))
    f.writerow([url_county])

def writerErrorFile(linkError,pageCount):
    f = csv.writer(open('error_url.csv', 'a', newline=''))
    f.writerow([linkError,pageCount])

def readFile():
    csvfile = open("page_url.csv", "r")
    table = csv.reader(csvfile)
    reader = []
    for row in table:
        reader.append(row)
    return reader

def insertJobCompany(title,thumb,description,requirement,why_youll_love,create_date,
                     posted_date,url_job,company_name,basic_info,product_type,
                     employees_numbers,country,working_date,overtime,url_company,page_count):
    logger = getLogging()
    try :
        connection = getConnection()
        cursor = connection.cursor()
        try:
            insert_sql = """insert into job_company(crawl_source,title,thumb,description,requirement,why_youll_love,
             create_date,posted_date,url_job,company_name,basic_info,product_type,employees_numbers,
             country,working_date,overtime,url_company) 
             values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
             
            insert_tuple = ('itviec',title,thumb,description,requirement,why_youll_love,create_date,posted_date,
                        url_job,company_name,basic_info,product_type,employees_numbers,
                        country,working_date,overtime,url_company)
            cursor.execute(insert_sql, insert_tuple)        
            connection.commit()  
        except Exception as error:
            try:
                update_sql = """update job_company set title=%s,thumb=%s,description=%s,requirement=%s,why_youll_love=%s,
             create_date=%s,posted_date=%s,company_name=%s,basic_info=%s,product_type=%s,employees_numbers=%s,
             country=%s,working_date=%s,overtime=%s,url_company=%s where url_job=%s"""
                insert_tuple = (title,thumb,description,requirement,why_youll_love,create_date,posted_date,
                        company_name,basic_info,product_type,employees_numbers,
                        country,working_date,overtime,url_company,url_job)
                cursor.execute(update_sql, insert_tuple)        
                connection.commit() 
            except mysql.connector.Error as error:
                logger.error('processor-3 :'+str(error)+'-'+url_job)
                writerErrorFile(url_job,page_count-1)
            except Exception as error:
                logger.error('processor-4 :'+str(error)+'-'+url_job)
                writerErrorFile(url_job,page_count-1)
    except Exception as error:
        logger.error('processor-6 :no connection :'+str(error))


    
    
