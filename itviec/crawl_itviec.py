# -*- coding: utf-8 -*-
import sys, csv, logging, datetime, re, requests
from bs4 import BeautifulSoup
from urllib.request import Request, urlopen
from datetime import datetime
from processor import (
    distanceTime,  readFile, writeFile, writerErrorFile, getLogging, insertJobCompany
)

create_date = datetime.now()
logger = getLogging()
logger.info('======== RUN CRAWL DATA-'+str(create_date)+' =========')
if readFile():
    job_url_file = readFile()[0][0]
    page_count = readFile()[0][1]
else:
    job_url_file = ''
    page_count = 1

while int(page_count) > 0:
    url = 'https://itviec.com/it-jobs?page=%d'%(int(page_count))
    page_count = int(page_count) + 1
    content =requests.get(url)
    soup = BeautifulSoup(content.text, 'html.parser')
    first_group = soup.find('div', class_='first-group').find_all('h2', class_='title')
    if first_group == []:
        break
    # check url job    
    for i in range(0,len(first_group)):
        url_check = 'https://itviec.com'+first_group[i].find('a').get('href')
        if url_check == job_url_file: 
            cuont = i 
            break
        else : cuont = 0

    for details in first_group[cuont+1:len(first_group)]:
        job_body = details.find('a').get('href')
        # url_job = 'https://itviec.com'+job_body
        url_job = 'https://itviec.com/it-jobs/voip-systems-administrator-personify-incorporation-2614'
        # content =requests.get(url_job)    
        # soup = BeautifulSoup(content.text, 'html.parser')
        page = urlopen(url_job, timeout=120)
        soup = BeautifulSoup(page, 'html.parser')
        #field job company
        try: 
            job_title = soup.find('div', class_='main-entity').find('div', class_='job-detail')
            title = job_title.find('div', {'class':'job_info'}).find('h1').text.split('\n')[1]
            skill = job_title.find('div', class_='tag-list').find_all('span')
            description = job_title.find('div', class_='job_description').find('div', class_='description').find('p')
            requirement = job_title.find('div', class_='skills_experience').find('div', class_='experience').find('p')
            try:
                why_youll_love = str(job_title.find('div', class_='love_working_here').find('div', class_='culture_description').find('p'))
            except :
                try:
                    top_reasons = job_title.find('div', class_='job_reason_to_join_us').find('div', class_='top-3-reasons').find('p')
                    if top_reasons:
                        why_youll_love = str(top_reasons)
                    else :
                        why_youll_love = None
                except:
                    why_youll_love = None
            try:
                str_time = job_title.find('div', class_='distance-time-job-posted highlight').get_text().split('\n')[2]
            except:
                str_time = job_title.find('div', class_='distance-time-job-posted').get_text().split('\n')[2]
            public_time = distanceTime(str_time)
            posted_date  = datetime.fromtimestamp(public_time)
            # field company
            thumb = soup.find('div', {'class':'logo'}).find('img').get('src')
            employer_info = soup.find('div', {'class':'employer-info'}).find('h3', class_='name')
            url_company = 'https://itviec.com'+employer_info.find('a').get('href')
            company_name = employer_info.find('a').get_text()
            div_basic = soup.find('div', class_='basic-info')
            basic_info = div_basic.find('div', class_='short').text.split('\n')[1]
            product_type = div_basic.find('p', class_='gear-icon').text.split('\n')[1]
            employees_numbers = div_basic.find('p', class_='group-icon').text.split('\n')[1]
            country = div_basic.find('div', class_='country-icon').find('span', class_='country-name').text
            try :
                working_date = div_basic.find('div', class_='working-date').find('span').text.split('\n')[1]
            except:
                working_date = None
            try:
                overtime = div_basic.find('div', class_='overtime').find('span').text.split('\n')[1]
            except :
                overtime = None
            
            try:
                insertJobCompany(title,thumb,str(description),str(requirement),why_youll_love,create_date,posted_date,
                                 url_job,company_name,basic_info,product_type,employees_numbers,
                                 country,working_date,overtime,url_company,page_count)
            except Exception as error:
                logger.error('crawl-2 :'+str(error)+'-'+url_job)
            writeFile(url_job,page_count-1)
        except Exception as error:
            logger.error('crawl-3 :'+str(error)+'-'+url_job)
            writerErrorFile(url_job,page_count-1)
        
