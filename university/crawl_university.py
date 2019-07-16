import sys, csv, pymysql.cursors
import requests
from bs4 import BeautifulSoup
import urllib.request
from constant import CITY 


#open write file
write = open('university_url.csv', 'a')
write_outfile = csv.writer(write)

#open read file
csvfile = open("university_url.csv", "r")
table = csv.reader(csvfile)
reader = []
for row in table:
    reader.append(row)

#connect database
connect_db = pymysql.connect(host="192.168.99.100",
                             port=3306, 
                             user="admin", 
                             db="istation",
                             password="12345678@", 
                             charset="utf8", 
                             cursorclass=pymysql.cursors.DictCursor )
for page in range(0,6):
    headers = {'x-requested-with':'XMLHttpRequest'}
    files = {'DiemchuanForm[page]': page ,
            'DiemchuanForm[universityName]': '' ,
            'DiemchuanForm[areas]': 0,
            'DiemchuanForm[provinceId]': 0,
            'DiemchuanForm[trainingType]': 0,
            'DiemchuanForm[admission]': 0,
            'DiemchuanForm[trainingMethod]': 0,
            'DiemchuanForm[groupSubject]': 0,
            'DiemchuanForm[mainMajor]': 0}
    url ='https://kenhtuyensinh.com.vn/university/viewmore'
    content =requests.post(url ,headers=headers ,data=files)
    soup = BeautifulSoup(content.text, 'html.parser')
    list_tag = soup.find('div', class_='scroll-result row_list').find_all('a', class_='name')

    for a_tag in list_tag:
        url =  'https://kenhtuyensinh.com.vn/'+ a_tag.get('href')
        flag = False

        # check url 
        for index in range(len(reader)):
            if url in reader[index]:
                flag = True
        if flag:
            continue
        else :
            write_outfile.writerow([url])
            page = urllib.request.urlopen(url)
            soup = BeautifulSoup(page, 'html.parser')
            div_tag =soup.find('div', class_='col-lg-9 right')
            name = div_tag.find('h4').get_text()
            school_code = div_tag.find('p', class_='cate-name').get_text().split(':')[1]
            english_name = div_tag.find('p', class_='name-en').find('span', class_='name-title').get_text()
            address = div_tag.find('p', class_='address').find('span', class_='name-title address2').get_text()
            try:
                name_abbreviation = div_tag.find('p', class_='cate-name2').get_text()
            except :
                name_abbreviation = 'null'          
            for p_tag in div_tag.find_all('p', class_='address'):
                city = p_tag.find('span', class_='name-title address2').get_text()
                for row in CITY :
                    if city == row[1] : city_index = row[0]

            # insert data 
            try:
                cursor = connect_db.cursor()
                sql = " insert into universitys_university(name,school_code,address,"\
                    + "name_abbreviation,english_name,city) "\
                    + "values (%s,%s,%s,%s,%s,%s)"
                cursor.execute(sql,(name,school_code,address,name_abbreviation,english_name,city_index))
                connect_db.commit()
            except:
                connect_db.rollback()
write.close()
csvfile.close()

        

