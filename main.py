import datetime
import threading
import multiprocessing
import time

import data_parser.parser as parser
import database as db
import logs
import os.path
import database.tables_managament as tables

from selenium.common.exceptions import InvalidSessionIdException
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


TIMEOUT_SECONDS = 120


# функция для запуска в отдельном потоке, которая проверяет находиться ли драйвер больше чем duration секунд
# на одной и той же странице, в случае если драйвер "завис" на одной и той же странице - вызывает driver.close()
def timeout_validation(driver, duration=TIMEOUT_SECONDS):
    old_url = driver.current_url
    time.sleep(duration)

    if driver.current_url == old_url:
        '''print(f'[TIMEOUT VALIDATION] ({threading.current_thread().name}) '
             f'Driver was closed as the page processing time exceeded. ({old_url})') '''

        logs.log_warning(f'[TIMEOUT VALIDATION] ({threading.current_thread().name}) '
                         f'Driver was closed as the page processing time exceeded. {old_url}')

        driver.close()
        # raise TimeoutError("Driver timeout")
    else:
        logs.log_info(f'[TIMEOUT VALIDATION] ({threading.current_thread().name}) '
                      f'The driver successfully processed the page within the specified time. '
                      f'{old_url}')


# получение ссылок на товары, находящиеся в categories_urls_files, их парсинг и запись в БД
def parsing_process(categories_urls_file, products_urls_file, proxy_data):
    parser.get_target_urls(categories_urls_file, products_urls_file, proxy_data=proxy_data)
    driver = parser.create_driver(images_enabled=True, proxy_data=proxy_data)
    driver.get('https://duckduckgo.com')

    try:
        # активные ссылки, которые находятся на сайте 168che
        with open(os.path.join(parser.BASE_DIR, 'data_parser', 'links', products_urls_file)) as r:
            active_links = r.read().split()

        # невалидные ссылки
        with open(os.path.join(parser.BASE_DIR, 'data_parser', 'links', 'invalid_links.txt')) as r:
            invalid_links = r.read().split()

        # локальные ссылки из БД
        local_links = db.execute_querry('SELECT url FROM vehicles_data;', True)

        urls_to_parse = (set(active_links) - set(local_links)) - set(invalid_links)

        current_process_name = multiprocessing.current_process().name
        total_products_num = len(urls_to_parse)
        successful_prods_count = 0

        for url in urls_to_parse:
            try:
                print(f'[PARSING PROCESS] ({current_process_name}) {successful_prods_count}/{total_products_num} '
                      f'Processing product with URL: {url}')
                driver.execute_script('window.location.href = arguments[0];', url)

                # отдельный поток для перехода на новый товар, если текущий обрабатывается уже более TIMEOUT_SECONDS
                timeoutValThread = threading.Thread(target=timeout_validation, name='TimeoutValidationThread',
                                                    args=(driver, TIMEOUT_SECONDS))
                timeoutValThread.start()

                product_data = parser.get_data(driver, url)

                if product_data:
                    db.write_productdata_to_db(product_data)
                    successful_prods_count += 1
                    print(f'[PARSING PROCESS] ({current_process_name}) {successful_prods_count}/{total_products_num} '
                          f'Data was obtained successfully!\n{product_data}')
                else:
                    invalid_links_file = open(os.path.join(parser.BASE_DIR, 'data_parser', 'links',
                                                           'invalid_links.txt'), 'a+', encoding='utf-8')
                    invalid_links_file.write(url + "\n")
                    invalid_links_file.close()
                    total_products_num -= 1
            except InvalidSessionIdException:
                total_products_num -= 1
                print('[PARSING PROCESS] Driver timeout.')
                logs.log_info('[PARSING PROCESS] Driver timeout.')
                print('[PARSING RPOCESS] Recreating driver.')
                logs.log_info('[PARSING RPOCESS] Recreating driver.')
                driver = parser.create_driver(images_enabled=True, proxy_data=proxy_data)
                driver.get('https://duckduckgo.com')
            except Exception as ex:
                print(f'[PARSING PROCESS] Driver was emergency closed ({ex})')
                # если ошибочно закрылись все окна браузера или сам браузер был аварийно закрыт, начинаем новую сессию
                if len(driver.window_handles) == 0 or driver is None or driver.service.process is None:
                    print('[PARSING RPOCESS] Recreating driver.')
                    logs.log_info('[PARSING RPOCESS] Recreating driver.')
                    driver = parser.create_driver(images_enabled=True, proxy_data=proxy_data)
                    driver.get('https://duckduckgo.com')
    except Exception as ex:
        print(ex)
        logs.log_warning(ex)
        send_notification(f'[KITAY AVTO] Критическая ошибка! ({ex}).')
    finally:
        parser.kill_driver(driver)


if __name__ == '__main__':
    logs.setup_logging()

    # список прокси
    raw_proxies = list(map(str.rstrip, open(os.path.join('data_parser', 'proxies.txt')).readlines()))
    proxy_list = []
    for data in raw_proxies:
        ip, port, login, password = data.split(":")
        proxy_list.append({'host': ip, 'port': port, 'login': login, 'password': password})

    TOTAL_PARSING_PROCESSES_NUM = 3
    while len(proxy_list) <= TOTAL_PARSING_PROCESSES_NUM:
        proxy_list.append(None)

    # поток для валидации активности
    activValThread = threading.Thread(target=activity_validation, name='ActivityValidationThread')

    parserProcess1 = multiprocessing.Process(target=parsing_process,
                                             args=('categories_urls_to_parse_1.txt', 'product_urls_to_parse_1.txt',
                                                   proxy_list[2]), name='parserProccess1')

    parserProcess2 = multiprocessing.Process(target=parsing_process,
                                             args=('categories_urls_to_parse_2.txt', 'product_urls_to_parse_2.txt',
                                                   proxy_list[1]), name='parserProccess2')
    parserProcess3 = multiprocessing.Process(target=parsing_process,
                                             args=('categories_urls_to_parse_3.txt', 'product_urls_to_parse_3.txt',
                                                   proxy_list[2]), name='parserProccess3')
    try:
        begin_session_time = datetime.datetime.now()
        # activValThread.start()
        send_notification(f'[KITAY AVTO] Запущено обновление активности ({datetime.datetime.now()})')

        parserProcess1.start()
        # parserProcess2.start()
        # parserProcess3.start()

        # activValThread.join()
        print('Обновление активности завершено.')
        logs.log_warning('Обновление активности завершено.')
        send_notification(f'[KITAY AVTO] Обновление активности завершено ({datetime.datetime.now()})')

        # parserProcess1.join()
        print('Первый процесс парсинга завершён.')
        # parserProcess2.join()
        print('Второй процесс парсинга завершён.')
        # parserProcess3.join()
        print('Третий процесс парсинга завершён.')
        logs.log_info('Парсинг завершён.')

        send_notification(f'[KITAY AVTO] Парсинг завершён! ({datetime.datetime.now()})')

        parser.delete_tmp_imgs()
        tables.write_data_to_xlsx('SELECT * FROM vehicles_data;', 'KitayAvto_output.xlsx')
        # tables.upload_file_to_ftp('KitayAvto_output.xlsx')
        end_session_time = datetime.datetime.now()

        send_notification(f'[KITAY AVTO] Данные успешно отправлены на FTP! {datetime.datetime.now()}')
        send_notification(f'[KITAY AVTO] Сессия завершена за {(end_session_time - begin_session_time).seconds / 3600}'
                          f' ч.')
    except Exception as _ex:
        print(_ex)
