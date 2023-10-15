import datetime

import data_parser.parser as parser
from database.postgres_connector import write_productdata_to_db

from logs import setup_logging


if __name__ == '__main__':
    setup_logging()
    driver = parser.create_driver(images_enabled=True)
    begin = datetime.datetime.now()
    driver.get(parser.HOST)
    # driver.fullscreen_window()
    with open('data_parser/links/product_urls_to_parse.txt', 'r', encoding='utf-8') as r:
        test_url = r.read().split()
    curr_product = 0
    all_prods = len(test_url)
    for url in test_url:
        try:
            driver.execute_script('window.location.href = arguments[0];', url)
            driver.execute_script("window.scrollBy(0, 15000);")
            driver.execute_script("window.scrollBy(0, -15000);")
            data = parser.get_data(driver, url)
            if data:
                write_productdata_to_db(data)
                print(f'[{curr_product}/{all_prods}] {data}')
                curr_product += 1
        except:
            all_prods-= 1
    end = datetime.datetime.now()
    parser.kill_driver(driver)

    print(begin, end)


