import sys, csv, logging
import mysql.connector
from bs4 import BeautifulSoup
from urllib.request import Request, urlopen
from retry import retry

def getLogging():
    LOG_FORMAT = "%(asctime)s [%(levelname)s]- %(message)s"
    logging.basicConfig(filename = 'access_log.log',
                        level = logging.DEBUG,
                        format = LOG_FORMAT)
    logger = logging.getLogger()
    return logger

@retry(TimeoutError,tries=3, delay=60, logger=getLogging())
def converterHtml(link):
    req = Request(link, headers={'User-Agent': 'Mozilla/5.0'})
    page = urlopen(req).read()
    soup_html = BeautifulSoup(page, 'html.parser')
    return soup_html

def writeFile(city_href,county_herf, pageCount):
    f = csv.writer(open('page_url.csv', 'w'))
    f.writerow([city_href, county_herf, pageCount])

def writerErrorFile(linkError,city_href,pageCount):
    f = csv.writer(open('error_url.csv', 'a', newline=''))
    f.writerow([linkError,city_href,pageCount])

def readFile():
    csvfile = open("page_url.csv", "r")
    table = csv.reader(csvfile)
    reader = []
    for row in table:
        reader.append(row)
    return reader

def getConnection():
    connection = mysql.connector.connect(host="localhost",
                                         port=3306, 
                                         user="root", 
                                         db="companies",
                                         password="", 
                                         charset="utf8" )
    return connection

def joinSquence(string):
    split_str = string.split('-')
    string_one = '-'
    string_two = [split_str[2],split_str[1], split_str[0]]
    return string_one.join(string_two)