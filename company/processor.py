import sys, csv, logging
import mysql.connector
from bs4 import BeautifulSoup
from urllib.request import Request, urlopen

def converterHtml(link):
    req = Request(link, headers={'User-Agent': 'Mozilla/5.0'})
    page = urlopen(req).read()
    soup_html = BeautifulSoup(page, 'html.parser')
    return soup_html

def writeFile(href, pageCount):
    f = csv.writer(open('company_data.csv', 'w'))
    f.writerow([href, pageCount])

def writerErrorFile(linkError):
    f = csv.writer(open('error_url.csv', 'a', newline=''))
    f.writerow([linkError])

def readFile():
    csvfile = open("company_data.csv", "r")
    table = csv.reader(csvfile)
    reader = []
    for row in table:
        reader.append(row)
    return reader

def getConnection():
    connection = mysql.connector.connect(host="192.168.99.100",
                                         port=3306, 
                                         user="admin", 
                                         db="istation",
                                         password="12345678@", 
                                         charset="utf8" )
    return connection

def getLogging():
    LOG_FORMAT = "[%(levelname)s] [%(asctime)s] - %(message)s"
    logging.basicConfig(filename = 'access_log.log',
                        level = logging.DEBUG,
                        format = LOG_FORMAT)
    logger = logging.getLogger()
    return logger