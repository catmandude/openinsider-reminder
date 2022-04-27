import logging
import time
from datetime import datetime
from urllib.parse import urljoin
import requests
from bs4 import BeautifulSoup
import sqlite3
from flask import Flask
from flask_sqlalchemy import SQLAlchemy

from sqlalchemy.sql import text

app = Flask(__name__)

# change to name of your database; add path if necessary
db_name = 'stocks.db'
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///' + db_name
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = True
db = SQLAlchemy(app)


class CLevelStock(db.Model):
    id = db.Column('c_level_stock_id', db.Integer, primary_key=True)
    x = db.Column(db.String(20))
    filing_date = db.Column(db.String(100))
    trade_date = db.Column(db.String(50))
    ticker = db.Column(db.String(100))
    company_name = db.Column(db.String(100))
    insider_name = db.Column(db.String(100))
    title = db.Column(db.String(100))
    trade_type = db.Column(db.String(100))
    price = db.Column(db.String(50))
    qty = db.Column(db.String(100))
    owned = db.Column(db.String(100))
    delta_owned = db.Column(db.String(50))
    value = db.Column(db.String(100))

    def __init__(self, x, filing_date, trade_date, ticker, company_name, insider_name, title, trade_type, price, qty, owned, delta_owned, value):
        self.x = x
        self.filing_date = filing_date
        self.trade_date = trade_date
        self.ticker = ticker
        self.company_name = company_name
        self.insider_name = insider_name
        self.title = title
        self.trade_type = trade_type
        self.price = price
        self.qty = qty
        self.owned = owned
        self.delta_owned = delta_owned
        self.value = value


logging.basicConfig(
    format='%(asctime)s %(levelname)s:%(message)s',
    level=logging.INFO)


class CrawlCheckStock:
    def __init__(self, urls=[]):
        self.visited_urls = []
        self.urls_to_visit = urls

    def download_url(self, url):
        return requests.get(url).text

    def crawl(self, url):
        html = self.download_url(url)

        soup = BeautifulSoup(html, "html.parser")

        results = soup.findChildren('table', attrs={"class": "tinytable"})

        # This will get the first (and only) table. Your page may have more.
        my_table = results[0]

        # You can find children with multiple tags by passing a list of strings
        rows = my_table.findChildren(['tr'])

        all_stocks = CLevelStock.query.all()

        for row in rows:
            heads = row.findChildren('h3')
            bodies = row.findChildren('td')

            tempArr = []

            for cell in bodies:
                tempArr.append(cell.getText().strip())

            if len(tempArr) > 0:
                x = tempArr[0]
                filing_date = tempArr[1]
                trade_date = tempArr[2]
                ticker = tempArr[3]
                company_name = tempArr[4]
                insider_name = tempArr[5]
                title = tempArr[6]
                trade_type = tempArr[7]
                price = tempArr[8]
                qty = tempArr[9]
                owned = tempArr[10]
                delta_owned = tempArr[11]
                value = tempArr[12]
                matches = next((stock for stock in all_stocks if stock.filing_date == filing_date and stock.company_name == company_name), None)
                if(matches == None):
                    thing = CLevelStock(x, filing_date, trade_date, ticker, company_name, insider_name, title, trade_type, price, qty, owned, delta_owned,value)
                    db.session.add(thing)
                    db.session.commit()


    def run(self):
        while self.urls_to_visit:
            url = self.urls_to_visit.pop(0)
            logging.info(f'Crawling: {url}')
            try:
                self.crawl(url)
            except Exception:
                logging.exception(f'Failed to crawl: {url}')
            finally:
                self.visited_urls.append(url)

@app.route('/insiders', methods=['GET'])
def insiders():
    all_stocks = CLevelStock.query.all()
    for stock in all_stocks:
        print(stock.company_name)
    return 'Hello'

if __name__ == '__main__':
    db.create_all()
    rate_sec = 10
    time_start = time.time()

    while True:
        print("{}".format(datetime.now()))

        CrawlCheckStock(urls=['http://openinsider.com/top-insider-sales-of-the-day']).run()

        # Sleep until the next 'rate_sec' multiple
        delay = rate_sec - (time.time() - time_start) % rate_sec
        time.sleep(delay)

    app.run(debug=True)

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
