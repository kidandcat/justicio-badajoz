"""
This script scrapes the Pontevedra city council website to get the normative documents.
Tested with Python 3.12.4

Run local db:
    $ docker run --name justicio-mysql -p 3306:3306 -e MYSQL_ROOT_PASSWORD=password -e MYSQL_DATABASE=justicio -d mysql:latest
Create table:
    $ docker exec -i justicio-mysql mysql -uroot -ppassword justicio < justicio.sql
Run script:
    $ python3 pontevedra.py
Backup:
    $ docker exec -i justicio-mysql /usr/bin/mysqldump -uroot  -ppassword justicio > backup.sql
"""
import pdftotext
from bs4 import BeautifulSoup
from urllib.request import urlopen
from datetime import datetime
import mysql.connector

# Constants
BASE_URL = "https://www.depo.gal/es/normativa-consolidada"
CITY = 'Pontevedra'
DATE = datetime.today().strftime('%Y-%m-%d')

# Connect to the database
mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="password",
    database="justicio"
)
mycursor = mydb.cursor()

# Start scraping
errors = 0
processed = 0
page = urlopen(BASE_URL)
html = page.read().decode("utf-8")
soup = BeautifulSoup(html, "html.parser")

sections = soup.select("div.accordion details")
if not sections:
    print("No sections found")
    exit()

for group in sections:
    groupname = group.select_one('summary strong').text
    links = group.select("ul li")
    if not links:
        print("No links found in", groupname)
        continue
    for link in links:
        try:
            title = link.text
            linka = link.find("a")
            url = linka.get("href")
            content = ''
            if '.pdf' in url:
                pdfdata = urlopen(url)
                pdf = pdftotext.PDF(pdfdata)
                content = "\n\n".join(pdf)
            else:
                linkpage = urlopen(url)
                linkhtml = linkpage.read().decode("utf-8")
                linksoup = BeautifulSoup(linkhtml, "html.parser")
                content_div = linksoup.select_one('#contAnuncio div[xmlns="http://www.w3.org/1999/xhtml"]')
                content = content_div.text
            
            sql = "INSERT INTO normativa (ciudad, date, titulo, grupo, subgrupo, url, content) VALUES (%s, %s, %s, %s, %s, %s, %s)"
            val = (CITY, DATE, title[0:150], groupname, '', url, content)
            mycursor.execute(sql, val)
            print("Processed", title, "in", groupname)
            processed += 1
        except Exception as e:
            errors += 1
            print("Error when processing", url, ":", e)

# Commit changes
mydb.commit()
print("Total errors", errors)
print("Total processed", processed, "items")