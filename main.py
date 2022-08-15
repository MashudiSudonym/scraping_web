import concurrent.futures
import threading
import json
import backoff as backoff
import requests
import time
import sqlite3
from datetime import datetime

import schedule
from bs4 import BeautifulSoup

thread_local = threading.local()
now = datetime.now()
start_time = time.time()
duration = time.time() - start_time


# back off strategy if connection error, untested, hope it's work.
@backoff.on_exception(backoff.expo,
                      requests.exceptions.RequestException,
                      max_tries=8,
                      jitter=None)
@backoff.on_exception(backoff.expo,
                      requests.exceptions.ConnectTimeout,
                      max_tries=8,
                      jitter=None)
@backoff.on_exception(backoff.expo,
                      requests.exceptions.ConnectionError,
                      max_tries=8,
                      jitter=None)
@backoff.on_exception(backoff.expo,
                      requests.exceptions.ChunkedEncodingError,
                      max_tries=8,
                      jitter=None)
@backoff.on_exception(backoff.expo,
                      requests.exceptions.HTTPError,
                      max_tries=8,
                      jitter=None)
@backoff.on_exception(backoff.expo,
                      requests.exceptions.ReadTimeout,
                      max_tries=8,
                      jitter=None)
def main(connect_db, cursor, snapshot_history):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:66.0) Gecko/20100101 Firefox/66.0",
        "Accept-Encoding": "*",
        "Connection": "keep-alive"
    }
    root_website_url = 'https://voila.id'
    page_param = 1
    pf_v_designers_param = [
        "/collections/a-p-c",
        "/collections/aape-by-bathing-ape",
        "/collections/adidas",
        "/collections/air-jordan",
        "/collections/alaia",
        "/collections/alessandra-rich",
        "/collections/alexander-mcqueen",
        "/collections/alexander-wang",
        "/collections/alexandre-mattiussi",
        "/collections/alo-yoga",
        "/collections/alpha-industries",
        "/collections/ambush",
        "/collections/amina-muaddi",
        "/collections/amiri",
        "/collections/andrea-adamo",
        "/collections/ann-demeulemeester",
        "/collections/aquazzura",
        "/collections/aries",
        "/collections/arizona-love",
        "/collections/asics",
        "/collections/au-depart",
        "/collections/balenciaga",
        "/collections/bally",
        "/collections/balmain",
        "/collections/barrow",
        "/collections/become-one",
        "/collections/billionaire-boys-club",
        "/collections/bottega-veneta",
        "/collections/boyy",
        "/collections/boyish",
        "/collections/burberry",
        "/collections/by-far",
        "/collections/canada-goose",
        "/collections/celine",
        "/collections/chanel",
        "/collections/chiara-ferragni",
        "/collections/chinatown-market",
        "/collections/chloe",
        "/collections/christian-dior",
        "/collections/christian-louboutin",
        "/collections/coach",
        "/collections/comme-des-garcons",
        "/collections/diesel",
        "/collections/dolce-gabbana",
        "/collections/domrebel",
        "/collections/dsquared2",
        "/collections/emporio-armani",
        "/collections/enterprise-japan",
        "/collections/evisu",
        "/collections/fear-of-god",
        "/collections/fendi",
        "/collections/franck-muller",
        "/collections/fred-perry",
        "/collections/fossil",
        "/collections/furla",
        "/collections/gcds",
        "/collections/gianvito-rossi",
        "/collections/giuseppe-zanotti",
        "/collections/givenchy",
        "/collections/gucci",
        "/collections/goyard",
        "/collections/haus-of-honey",
        "/collections/hermes",
        "/collections/heron-preston",
        "/collections/hublot",
        "/collections/hugo-boss",
        "/collections/ih-nom-uh-nit",
        "/collections/isabel-marant",
        "/collections/jacquemus",
        "/collections/junya-watanabe",
        "/collections/jil-sander",
        "/collections/jimmy-choo",
        "/collections/jw-anderson",
        "/collections/kappa",
        "/collections/karl-lagerfeld",
        "/collections/kenzo",
        "/collections/khaite",
        "/collections/kurt-geiger",
        "/collections/lamborghini",
        "/collections/lanvin",
        "/collections/loewe",
        "/collections/longchamp",
        "/collections/louis-vuitton-2",
        "/collections/mach-mach",
        "/collections/maison-kitsune",
        "/collections/maison-margiela",
        "/collections/maje",
        "/collections/malone-souliers",
        "/collections/manolo-blahnik",
        "/products/manoush-robe-midi-dress-majeste-black",
        "/collections/marc-jacobs",
        "/collections/marcelo-burlon",
        "/collections/marni",
        "/collections/mastermind",
        "/collections/mc2-saint-barth",
        "/collections/mcm",
        "/collections/michael-kors",
        "/collections/mido",
        "/collections/mini-rodini",
        "/collections/miu-miu",
        "/collections/missoni",
        "/collections/mlb",
        "/collections/moncler",
        "/collections/moschino",
        "/collections/msgm",
        "/collections/neil-barrett",
        "/collections/nike",
        "/collections/off-white",
        "/collections/oris",
        "/collections/paco-rabanne",
        "/collections/palm-angels",
        "/collections/pas-de-mer",
        "/collections/patek-philippe",
        "/collections/paul-shark",
        "/collections/pharmacy-industry",
        "/collections/phillip-lim",
        "/collections/philipp-plein",
        "/collections/philosophy-di-lorenzo",
        "/collections/pinko",
        "/collections/polo-ralph-lauren",
        "/collections/prada",
        "/collections/proenza-schouler",
        "/collections/rails",
        "/collections/red-valentino",
        "/collections/re-done",
        "/collections/rick-owens",
        "/collections/rodebjer",
        "/collections/rodo",
        "/collections/roger-vivier",
        "/collections/rolex",
        "/collections/sacai",
        "/collections/saint-laurent",
        "/collections/salvatore-ferragamo",
        "/collections/see-by-chloe",
        "/collections/seiko",
        "/collections/self-portrait/Self-portrait",
        "/collections/sergio-rossi",
        "/collections/sprayground",
        "/collections/staud",
        "/collections/stella-mccartney",
        "/collections/steve-madden",
        "/collections/stuart-weitzman",
        "/collections/stussy",
        "/collections/theory",
        "/collections/tods",
        "/collections/thom-browne",
        "/collections/tissot",
        "/collections/tory-burch",
        "/collections/tumi",
        "/collections/twinset",
        "/collections/ulla-johnson",
        "/collections/valentino-garavani",
        "/collections/van-cleef-arpels",
        "/collections/versace",
        "/collections/vetements",
        "/collections/vien",
        "/collections/yuzefi",
        "/collections/y-3",
        "/collections/zimmermann",
    ]
    session = get_session()

    print("Current Time =", now)

    # Loop designers list
    for index, designer in enumerate(pf_v_designers_param):
        # Loop true if designers param have a pagination
        while True:
            with session.get(url=f'{root_website_url}{designer}?page={page_param}', headers=headers,
                             stream=True) as response:
                root_soup = BeautifulSoup(response.content, 'html5lib')
                root_result = root_soup

                print(f'{index + 1}. {root_website_url}{designer}?page={page_param}')

                items = root_result.find_all('div', {'class': 'boost-pfs-filter-product-item '
                                                              'boost-pfs-filter-product-item-grid '
                                                              'has-bc-swap-image boost-pfs-filter-grid-width-3 '
                                                              'boost-pfs-filter-grid-width-mb-2 on-sale'})

                # Loop items list
                for index_item, item in enumerate(items):
                    detail_item_link = item.find('a', {'class': 'boost-pfs-filter-product-item-image-link lazyload '
                                                                'boost-pfs-filter-crop-image-position-none'})[
                        'href'].strip()

                    detail_item_url = f'{root_website_url}{detail_item_link}#'

                    print(f'#{designer.replace("/collections/", "")} | {index_item + 1}. {detail_item_url}')

                    new_product_index = len(cursor.execute('''SELECT * FROM tbl_voila_products''').fetchall())

                    if new_product_index == 0:
                        index_product = index_item + 1
                    else:
                        index_product = new_product_index + 1

                    detail_item_response = requests.get(url=detail_item_url)
                    detail_item_soup = BeautifulSoup(detail_item_response.content, 'html5lib')
                    script = detail_item_soup.find('script', {'class': 'wcp_json'})
                    script_content = None if script is None else script.contents[0].text.strip()
                    json_data = json.loads(script_content)

                    get_product_title = json_data['title']
                    get_product_available_status = json_data['available']
                    get_product_price = json_data['price']
                    get_product_price_before_sale = json_data['compare_at_price']
                    get_product_vendor = json_data['vendor']
                    get_product_variants = json_data['variants']

                    print(f'product title: {get_product_title}')
                    print(f'product is available: {get_product_available_status}')
                    print(f'product discount price: {get_product_price}')
                    print(f'product real price: {get_product_price_before_sale}')
                    print(f'product vendor: {get_product_vendor}')

                    cursor.execute(
                        '''INSERT INTO tbl_voila_products(title, available_status, discount_price, real_price, vendor, 
                        snapshot_history_id) 
                            VALUES(?,?,?,?,?,?)''', (
                            get_product_title, get_product_available_status, get_product_price,
                            get_product_price_before_sale, get_product_vendor, snapshot_history)
                    )

                    connect_db.commit()

                    for variant in get_product_variants:
                        if variant['title'] != 'Default Title':
                            get_product_variant_title = variant['title']
                            get_product_variant_available_status = variant['available']

                            print(f'product variant: {get_product_variant_title}')
                            print(f'product variant available: {get_product_variant_available_status}')

                            cursor.execute('''INSERT INTO tbl_voila_product_variants(product_id, variant, 
                            variant_available_status, snapshot_history_id) 
                                    VALUES(?,?,?,?)''', (
                                index_product, get_product_variant_title, get_product_variant_available_status,
                                snapshot_history))

                            connect_db.commit()
                        else:
                            cursor.execute('''INSERT INTO tbl_voila_product_variants(product_id, variant, 
                                                        variant_available_status, snapshot_history_id) 
                                                                VALUES(?,?,?,?)''', (
                                index_product, 'NULL', 0,
                                snapshot_history))

                            connect_db.commit()
                            print('This product has no variant')

                # pagination checking, if don't have a next page, break/stop looping.
                if len(items) != 0:
                    page_param += 1
                else:
                    page_param = 1
                    break

    connect_db.execute(
        '''INSERT INTO tbl_voila_scrape_data_status(snapshot_history_id, is_full_download, started_at) VALUES(?, ?, ?)''',
        (snapshot_history, 1, now))
    connect_db.commit()


def get_session():
    if not hasattr(thread_local, "session"):
        thread_local.session = requests.Session()
    return thread_local.session


def scrape_it():
    # SQLITE
    conn = sqlite3.connect('voila.db')
    c = conn.cursor()

    c.execute("PRAGMA foreign_keys = ON")
    c.execute('''CREATE TABLE IF NOT EXISTS
                tbl_voila_products(_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, title TEXT, available_status TEXT, 
                discount_price TEXT,
                real_price TEXT, vendor TEXT, 
                snapshot_history_id INTEGER,
                FOREIGN KEY(snapshot_history_id)
                REFERENCES tbl_snapshot_histories(_id) ON DELETE SET NULL ON UPDATE CASCADE)
                ''')
    c.execute('''CREATE TABLE IF NOT EXISTS
                    tbl_voila_product_variants(_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                    product_id INTEGER,
                    variant TEXT, variant_available_status TEXT,
                    snapshot_history_id INTEGER,
                    FOREIGN KEY(product_id) 
                    REFERENCES tbl_voila_products(_id) ON DELETE SET NULL ON UPDATE CASCADE,
                    FOREIGN KEY(snapshot_history_id) 
                    REFERENCES tbl_snapshot_histories(_id) ON DELETE SET NULL ON UPDATE CASCADE)
                    ''')
    c.execute('''CREATE TABLE IF NOT EXISTS tbl_voila_scrape_data_status(_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, 
        snapshot_history_id INTEGER,
        is_full_download TEXT, started_at DATE,
        FOREIGN KEY(snapshot_history_id)
                REFERENCES tbl_snapshot_histories(_id) ON DELETE SET NULL ON UPDATE CASCADE
                )''')
    c.execute('''CREATE TABLE IF NOT EXISTS tbl_snapshot_histories(_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, 
        snapshot TEXT)''')

    snapshot = len(c.execute('''SELECT * FROM tbl_snapshot_histories''').fetchall()) + 1

    c.execute('''INSERT INTO tbl_snapshot_histories(snapshot) VALUES(?)''', (snapshot,))
    conn.commit()

    # running main function with concurrent
    # noinspection PyBroadException
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            executor.submit(main(conn, c, snapshot))
    except (KeyboardInterrupt, ConnectionError, ConnectionResetError, Exception):
        c.execute(
            '''INSERT INTO tbl_voila_scrape_data_status(snapshot_history_id, is_full_download, started_at) VALUES(?, ?, ?)''',
            (snapshot, 0, now))
        conn.commit()

    # close db
    c.close()

    # show log how long scraping process.
    print(f"Downloaded data site in : {duration} seconds")


if __name__ == '__main__':
    scrape_it()
