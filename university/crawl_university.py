import sys, csv, requests, logging
from bs4 import BeautifulSoup
from urllib.request import urlopen, HTTPError
from constant import CITY
import mysql.connector
from mysql.connector import Error


# format file log
LOG_FORMAT = "[%(levelname)s] [%(asctime)s] - %(message)s"
logging.basicConfig(filename = 'geckodriver.log',   
                    level = logging.DEBUG,
                    format = LOG_FORMAT)
logger = logging.getLogger()

#open write file
write = open('university_url.csv', 'a',newline='')
write_outfile = csv.writer(write)

#open read file
csvfile = open("university_url.csv", "r")
table = csv.reader(csvfile)
reader = []
for row in table:
    reader.append(row)
try:
    #connect database
    connect_db = mysql.connector.connect(host="localhost",
                                         port=3306, 
                                         user="root", 
                                         db="universities",
                                         password="1234", 
                                         charset="utf8" )
        
    if (connect_db.is_connected()):
        db_Info = connect_db.get_server_info()
        logger.info('Connected to MySQL database')
    cursor = connect_db.cursor() 
    #check page
    if len(reader) <= 0: index_page = 0
    else : index_page=reader[len(reader)-1][1]

    while int(index_page) >= 0:
        headers = {'x-requested-with':'XMLHttpRequest'}
        files = {'DiemchuanForm[page]': int(index_page) ,
                'DiemchuanForm[universityName]': '' ,
                'DiemchuanForm[areas]': 0,
                'DiemchuanForm[provinceId]': 0,
                'DiemchuanForm[trainingType]': 0,
                'DiemchuanForm[admission]': 0,
                'DiemchuanForm[trainingMethod]': 0,
                'DiemchuanForm[groupSubject]': 0,
                'DiemchuanForm[mainMajor]': 0}
        url ='https://kenhtuyensinh.com.vn/university/viewmore'
        index_page = int(index_page) + 1 
        try :
            content =requests.post(url ,headers=headers ,data=files)
            soup = BeautifulSoup(content.text, 'html.parser')
            list_tag = soup.find('div', class_='scroll-result row_list').find_all('a', class_='name')
        except HTTPError as error:
            logger.info('1 :'+str(error))
            continue
        except Exception as error : 
            logger.error('2 :'+str(error))   
            break              
           
        for a_tag in list_tag:    
            url =  'https://kenhtuyensinh.com.vn/'+ a_tag.get('href')    
            flag = False
            # check url    
            for index in range(len(reader)):    
                if url == reader[0][index]:    
                    flag = True    
                    break    
            if flag:    
                continue    
            else :    
                try:    
                    page_html = urlopen(url)    
                    soup = BeautifulSoup(page_html, 'html.parser')    
                    div_tag =soup.find('div', class_='col-lg-9 right')    
                    name = div_tag.find('h4').get_text()    
                    school_code = div_tag.find('p', class_='cate-name').get_text().split(':')[1]    
                    english_name = div_tag.find('p', class_='name-en').find('span', class_='name-title').get_text()    
                    address = div_tag.find('p', class_='address').find('span', class_='name-title address2').get_text()    
                    try:    
                        name_abbreviation = div_tag.find('p', class_='cate-name2').get_text()    
                    except :    
                        name_abbreviation = None    
                    
                    #check city           
                    city = soup.find('div', class_='col-lg-12').find_all('p', class_='address')[1].find('a').get_text()         
                    for row in CITY :    
                        if city == row[1] :     
                            city_index = row[0]    
                            break    
                        else :    
                            city_index = None       
                except Exception as error:    
                    with open('url_error.csv', 'a',newline='') as url_error:    
                        write_error = csv.writer(url_error)    
                        write_error.writerow([url,index_page])                                       
                    logger.error('3 :'+str(error)+'-'+url)
                    continue
                    # insert data     
                try:        
                          
                    sql = """ insert into university(name,school_code,address,name_abbreviation,
                                english_name,city) values (%s,%s,%s,%s,%s,%s)"""        
                    cursor.execute(sql,(name,school_code,address,name_abbreviation,english_name,city_index))        
                    connect_db.commit()        
                    write_outfile.writerow([url,index_page-1])       
                except mysql.connector.Error as error :        
                    connect_db.rollback()        
                    with open('url_error.csv', 'a',newline='') as url_error:        
                        write_error = csv.writer(url_error)        
                        write_error.writerow([url,index_page])        
                    logger.warning(error)                      
                    logger.error('4 :'+str(error)+'-'+url)         
                except Exception as error:        
                    logger.error('5 :'+str(error))                                     
except Error as e:
    logger.error('no connection :'+str(e))
except Exception as error:
    logger.error('6 :'+str(error))
finally:
    if connect_db.is_connected():
        cursor.close()
        connect_db.close()
    logger.info('close the connection database')

write.close()
csvfile.close() 

