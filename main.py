import data_parser.parser as parser
from database.postgres_connector import write_productdata_to_db

from logs import setup_logging

TEST_URL = 'https://www.che168.com/dealer/263982/48579179.html?pvareaid=110520&userpid=500000&usercid=500100&offertype=&offertag=0&activitycartype=0&fromsxmlist=0#pos=2#page=1#rtype=10#isrecom=0#filter=29#module=10#refreshid=0#recomid=0#queryid=1694723125$B$fbe63028-07a6-486e-955c-f5e7619dd746$72989#cartype=70'


if __name__ == '__main__':
    setup_logging()
    # parser.get_target_urls()
    driver = parser.create_driver()
    driver.get(TEST_URL)
    driver.execute_script("window.scrollBy(0, 15000);")
    driver.execute_script("window.scrollBy(0, -15000);")

    with open('test_page_source.html', 'w', encoding='utf-8') as o:
        o.write(driver.page_source)
    data = parser.get_data(driver, TEST_URL)
    write_productdata_to_db(data)
