"""
This script scrapes the Badajoz city council website to get the normative documents.
Tested with Python 3.12.4

Run local db:
    $ docker run --name justicio-mysql -p 3306:3306 -e MYSQL_ROOT_PASSWORD=password -e MYSQL_DATABASE=justicio -d mysql:latest
Create table:
    $ docker exec -i justicio-mysql mysql -uroot -ppassword justicio < justicio.sql
Run script:
    $ python3 badajoz.py
Backup:
    $ docker exec -i justicio-mysql /usr/bin/mysqldump -uroot  -ppassword justicio > backup.sql
"""
import requests
import pdftotext
from bs4 import BeautifulSoup
from datetime import datetime
import mysql.connector
import io  # Add this import at the top of the file

# Constants
BASE_URL = "https://www.aytobadajoz.es/es/ayto/ordenanzas"
CITY = 'Badajoz'
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
response = requests.get(BASE_URL)
soup = BeautifulSoup(response.text, "html.parser")

# Find all links within items with class 'item_fichero'
normativas = soup.select('.item_fichero a')

if not normativas:
    print("No normativas found")
    exit()

print(f"Found {len(normativas)} potential normativa links")

for normativa in normativas:
    try:
        title = normativa.text.strip()
        url = normativa['href']
        
        # Ensure the URL is absolute
        if not url.startswith('http'):
            url = 'https://www.aytobadajoz.es' + url
        
        # Find the parent div for additional info
        parent_div = normativa.find_parent('div', class_='item_fichero')
        subgrupo = ''
        if parent_div:
            # Extract date or any other info from the 'fecha' div
            fecha_div = parent_div.find('div', class_='fecha')
            subgrupo = fecha_div.text.strip() if fecha_div else ''
        
        content = ''
        if '.pdf' in url:
            pdf_response = requests.get(url)
            pdf_content = io.BytesIO(pdf_response.content)
            pdf = pdftotext.PDF(pdf_content)
            content = "\n\n".join(pdf)
        else:
            content = "Non-PDF content. Please implement specific scraping logic."
        
        sql = "INSERT INTO normativa (ciudad, date, titulo, grupo, subgrupo, url, content) VALUES (%s, %s, %s, %s, %s, %s, %s)"
        val = (CITY, DATE, title[0:150], 'Ordenanzas', subgrupo, url, content)
        mycursor.execute(sql, val)
        print("Processed", title)
        processed += 1
    except Exception as e:
        errors += 1
        print("Error when processing", url, ":", e)

# Commit changes
mydb.commit()
print("Total errors", errors)
print("Total processed", processed, "items")