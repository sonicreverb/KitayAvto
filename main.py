import datetime
import threading
import multiprocessing
import data_parser.parser as parser
import database as db
import logs
import os.path
import database.tables_managament as tables

from telegram_alerts import send_notification


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
            db.execute_querry('UPDATE vehicles_data SET unactive_since = NOW() WHERE activity = false AND'
                              ' unactive_since is NULL')
    db.postgres_connector.delete_unactive_positions()
    print(f'[ACTIVITY VALIDATION] ({threading.current_thread().name}) Activity validation complete.')
    logs.log_info(f'[ACTIVITY VALIDATION] ({threading.current_thread().name}) Activity validation complete.')


# получение ссылок на товары, находящиеся в categories_urls_files, их парсинг и запись в БД
def parsing_process(categories_urls_file, products_urls_file):
    parser.get_target_urls(categories_urls_file, products_urls_file)
    driver = parser.create_driver(images_enabled=True)
    driver.get('https://duckduckgo.com')

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
            except Exception as ex:
                print(ex)
                # если ошибочно закрылись все окна браузера или сам браузер был аварийно закрыт, начинаем новую сессию
                if len(driver.window_handles) == 0 or driver is None or driver.service.process is None:
                    print('[PARSING RPOCESS] Recreating driver.')
                    logs.log_info('[PARSING RPOCESS] Recreating driver.')
                    driver = parser.create_driver(images_enabled=True)
                    driver.get('https://duckduckgo.com')
    except Exception as ex:
        print(ex)
        logs.log_warning(ex)
        send_notification(f'[KITAY AVTO] Критическая ошибка! ({ex}).')
    finally:
        parser.kill_driver(driver)


if __name__ == '__main__':
    logs.setup_logging()

    # поток для валидации активности
    activValThread = threading.Thread(target=activity_validation, name='ActivityValidationThread')

    parserProcess1 = multiprocessing.Process(target=parsing_process,
                                             args=('categories_urls_to_parse_1.txt', 'product_urls_to_parse_1.txt'),
                                             name='parserProccess1')

    parserProcess2 = multiprocessing.Process(target=parsing_process,
                                             args=('categories_urls_to_parse_2.txt', 'product_urls_to_parse_2.txt'),
                                             name='parserProccess2')
    try:
        begin_session_time = datetime.datetime.now()
        activValThread.start()

        parserProcess1.start()
        parserProcess2.start()

        activValThread.join()
        print('Обновление активности завершено.')
        logs.log_warning('Обновление активности завершено.')
        send_notification(f'[KITAY AVTO] Обновление активности завершено ({datetime.datetime.now()})')

        parserProcess1.join()
        parserProcess2.join()
        print('Парсинг завершён.')
        logs.log_info('Парсинг завершён.')

        send_notification(f'[KITAY AVTO] Парсинг завершён! ({datetime.datetime.now()})')

        tables.write_data_to_xlsx('SELECT * FROM vehicles_data;', 'KitayAvto_output.xlsx')
        tables.upload_file_to_ftp('KitayAvto_output.xlsx')
        end_session_time = datetime.datetime.now()

        send_notification(f'[KITAY AVTO] Данные успешно отправлены на FTP! {datetime.datetime.now()}')
        send_notification(f'[KITAY AVTO] Сессия завершена за {(end_session_time - begin_session_time).seconds / 3600}'
                          f' ч.')
    except Exception as _ex:
        print(_ex)
