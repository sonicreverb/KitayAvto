import threading
import multiprocessing
import data_parser.parser as parser
import database as db
import logs
# import telegram_alerts
import os.path


# валидация активностей товаров из БД
def activity_validation():
    print(f'[ACTIVITY VALIDATION] ({threading.current_thread().name}) Starting activity validation...')
    logs.log_info(f'[ACTIVITY VALIDATION] ({threading.current_thread().name}) Starting activity validation...')
    urls_to_validate = db.execute_querry('SELECT url FROM vehicles_data WHERE activity = true', data_returned=True)
    for url in urls_to_validate:
        activity = parser.validate_product_activity(url)
        if not activity:
            print(f'[ACTIVITY VALIDATION] ({threading.current_thread().name}) '
                  f'Setting false activity to product with URL: {url}')
            logs.log_info(f'[ACTIVITY VALIDATION] ({threading.current_thread().name})'
                          f' Setting false activity to product with URL: {url}')
            db.execute_querry(f'UPDATE vehicles_data SET activity = false WHERE url = \'{url}\'', data_returned=False)
    print(f'[ACTIVITY VALIDATION] ({threading.current_thread().name}) Activity validation complete.')
    logs.log_info(f'[ACTIVITY VALIDATION] ({threading.current_thread().name}) Activity validation complete.')


# получение ссылок на товары, находящиеся в categories_urls_files, их парсинг и запись в БД
def parsing_process(categories_urls_file, products_urls_file):
    parser.get_target_urls(categories_urls_file, products_urls_file)
    driver = parser.create_driver(images_enabled=True)
    driver.get(parser.HOST)

    try:
        with open(os.path.join(parser.BASE_DIR, 'data_parser', 'links', products_urls_file)) as r:
            urls_to_parse = r.read().split()

        current_process_name = multiprocessing.current_process().name
        total_products_num = len(urls_to_parse)
        successful_prods_count = 0

        for url in urls_to_parse:
            try:
                print(f'[PARSING PROCESS] ({current_process_name}) {successful_prods_count}/{total_products_num} '
                      f'Proseccing product with URL: {url}')
                driver.execute_script('window.location.href = arguments[0];', url)
                data = parser.get_data(driver, url)

                if data:
                    successful_prods_count += 1
                    db.write_productdata_to_db(data)
                    print(f'[PARSING PROCESS] ({current_process_name}) {successful_prods_count}/{total_products_num} '
                          f'Data was obtained successfully!\n{data}')
                else:
                    total_products_num -= 1
            except Exception as _ex:
                print(_ex)
    except Exception as _ex:
        print(_ex)
    finally:
        parser.kill_driver(driver)


if __name__ == '__main__':
    logs.setup_logging()

    # поток для валидации активности
    activValThread = threading.Thread(target=activity_validation, name='ActivityValidationThread')
    activValThread.start()

    parserProcess1 = multiprocessing.Process(target=parsing_process,
                                             args=('categories_urls_to_parse_1.txt', 'product_urls_to_parse_1.txt'),
                                             name='parserProccess1')

    parserProcess2 = multiprocessing.Process(target=parsing_process,
                                             args=('categories_urls_to_parse_2.txt', 'product_urls_to_parse_2.txt'),
                                             name='parserProccess2')

    parserProcess1.start()
    parserProcess2.start()

    activValThread.join()
    print('Обновление активности завершено.')
    logs.log_warning('Обновление активности завершено.')

    parserProcess1.join()
    parserProcess2.join()
    print('Парсинг завершён.')
    logs.log_info('Парсинг завершён.')
