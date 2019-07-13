import sys, csv, pymysql
import requests
from bs4 import BeautifulSoup
import urllib.request


list_url = ['https://itviec.com/jobs-company-index',
            'https://itviec.com/jobs-company-index/d-f',
            'https://itviec.com/jobs-company-index/g-i',
            'https://itviec.com/jobs-company-index/j-l',
            'https://itviec.com/jobs-company-index/m-o',
            'https://itviec.com/jobs-company-index/p-r',
            'https://itviec.com/jobs-company-index/s-v',
            'https://itviec.com/jobs-company-index/w-z']

# open file 
f = csv.writer(open('company_data.csv', 'a', encoding='utf-8'))
f.writerow(['name', 'locations','nation','outsourcing','product','working_schedule','overtime'])

# take url in list_url
for url in list_url :
    page = urllib.request.urlopen(url)
    soup = BeautifulSoup(page, 'html.parser')
    ul_tag = soup.find('div', class_='job-skill-indices').find_all('ul', class_='skill-tag__list double')

    # retrieve list tag a in tag ul
    for list_a in ul_tag:
        a_tag = list_a.find_all('a',class_='mkt-track skill-tag__link')

        # retrieve list href in tag a
        for href in a_tag:
            url = 'https://itviec.com' + href.get('href')
            page = urllib.request.urlopen(url)
            soup = BeautifulSoup(page, 'html.parser')
            div_tag = soup.find('div', class_='name-and-info')
           
            # retrieve data from html
            try:
                name = div_tag.find('h1').get_text().split('\n')[1]
            except :
                continue

            try:
                locations = div_tag.find('span').get_text().split('\n')[2]
            except :
                locations = 'null'
            try:
                nation  = div_tag.find('div', class_='country').find('span', class_='name').get_text()    
            except:
                nation  = 'null'
            try:
                company_type = div_tag.find('div', class_='company-info').find('span', class_='gear-icon').get_text().split('\n')[1]
                if company_type == 'Product' :
                    outsourcing = False
                    product = True
                else :
                    outsourcing = True
                    product = False
            except :
                outsourcing = False
                product = False
            try:
                working_schedule =  div_tag.find('div', class_='working-date').find('span').get_text().split('\n')[1]
            except :
                working_schedule = 'null'
            try:
                overtime = div_tag.find('div', class_='overtime').find('span').get_text().split('\n')[1]
                if overtime == 'No OT' : overtime = False
                else : overtime = True
            except :
                overtime = False   
                     
            # write file  company_data.csv
            f.writerow([name, locations, nation, outsourcing,product, working_schedule, overtime])
            
