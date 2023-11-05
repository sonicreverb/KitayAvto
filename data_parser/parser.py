import time
import requests
import zipfile
import selenium.common.exceptions
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.action_chains import ActionChains
from bs4 import BeautifulSoup
from googletrans import Translator
from pycbrf import ExchangeRates
from word2number import w2n
from multiprocessing import current_process
from database import execute_querry

import os
import logs
import re

HOST = 'https://www.che168.com'
BASE_DIR = os.path.dirname(os.path.dirname(__file__))

manifest_json = """
{
    "version": "1.0.0",
    "manifest_version": 2,
    "name": "Chrome Proxy",
    "permissions": [
        "proxy",
        "tabs",
        "unlimitedStorage",
        "storage",
        "<all_urls>",
        "webRequest",
        "webRequestBlocking"
    ],
    "background": {
        "scripts": ["background.js"]
    },
    "minimum_chrome_version":"22.0.0"
}
"""


# возвращает пустой драйвер
def create_driver(images_enabled=False, proxy_data=None):
    # оключение загрузки изображений для оптимизации
    chrome_options = Options()
    if not images_enabled:
        chrome_options.add_argument(f"--blink-settings=imagesEnabled=false")

    # подключение прокси
    if proxy_data:
        proxy_host = proxy_data.get('host')
        proxy_port = proxy_data.get('port')
        proxy_username = proxy_data.get('login')
        proxy_password = proxy_data.get('password')

        background_js = """
        var config = {
                mode: "fixed_servers",
                rules: {
                singleProxy: {
                    scheme: "http",
                    host: "%s",
                    port: parseInt(%s)
                },
                bypassList: ["localhost"]
                }
            };

        chrome.proxy.settings.set({value: config, scope: "regular"}, function() {});

        function callbackFn(details) {
            return {
                authCredentials: {
                    username: "%s",
                    password: "%s"
                }
            };
        }

        chrome.webRequest.onAuthRequired.addListener(
                    callbackFn,
                    {urls: ["<all_urls>"]},
                    ['blocking']
        );
        """ % (proxy_host, proxy_port, proxy_username, proxy_password)
        pluginfile = 'proxy_auth_plugin.zip'

        with zipfile.ZipFile(pluginfile, 'w') as zp:
            zp.writestr("manifest.json", manifest_json)
            zp.writestr("background.js", background_js)
        chrome_options.add_extension(pluginfile)

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    print('[DRIVER INFO] Driver created successfully.\n')
    logs.log_info('[DRIVER INFO] Driver created successfully.')
    return driver


# закрывает все окна и завершает сеанс driver
def kill_driver(driver):
    driver.close()
    driver.quit()
    print('[DRIVER INFO] Driver was closed successfully.\n')
    logs.log_info('[DRIVER INFO] Driver was closed successfully.')


# возвращает soup указанной страницы
def get_htmlsoup(driver):
    try:
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        return soup

    except Exception as _ex:
        print(f'[GET HTMLSOUP] An error occurred while trying to get soup - {_ex}.')
        logs.log_warning(f'[GET HTMLSOUP] An error occurred while trying to get soup - {_ex}.')
        return None


# возвращает перевдённый текст
def translate_text(text, target_language):
    translator = Translator()
    translated = translator.translate(text, dest=target_language)
    return translated.text


# возвращает текущий курс юань
def get_cny_rate():
    try:
        rates = ExchangeRates()
        cny_to_rub_rate = rates['CNY'].value
        return float(cny_to_rub_rate)
    except Exception as _ex:
        logs.log_error(f"[GET CNY RATE] Error while trying to get current course {_ex}")
        raise SystemExit(-1)


# возвращает словарь product_data с данными о товаре указанной страницы
def get_data(driver, url):
    current_process_name = current_process().name
    try:
        soup = get_htmlsoup(driver)
        if not soup:
            return None

        # прокрутка в самый низ страницы для загрузки изображений
        actions = ActionChains(driver)
        img_scroll_contentbox = driver.find_element(By.XPATH, '//*[@id="pic_li"]')
        actions.move_to_element(img_scroll_contentbox).perform()
        time.sleep(1)
        driver.execute_script("window.scrollBy(0, 1500);")

        # НАИМЕНОВАНИЕ ТОВАРА
        name = translate_text(soup.find('h3', class_='car-brand-name').get_text(), 'en')
        print(f'\n\n[GET DATA] ({current_process_name}) Current product - {name}, ({url}).')
        logs.log_info(f'[GET DATA] ({current_process_name}) Current product {name}, ({url}).')

        # проверка на дубликат
        if name in execute_querry('SELECT name FROM vehicles_data', True):
            print('[GET DATA] Error. Vehicle with this name already exist in DB.')
            logs.log_warning('[GET DATA] Error. Vehicle with this name already exist in DB.')
            return None

        # ЦЕНА
        price_ch = soup.find('span', class_='price').get_text()
        if not price_ch:
            print(f"[GET DATA] ({current_process_name}) Couldn't get product price ({url}).")
            logs.log_warning(f"[GET DATA] ({current_process_name}) Couldn't get product price ({url}).")
            return None
        # форматирование цены
        parsed_price = price_ch.replace(',', '').replace('\n', '').split('：')[0]
        # на сайте цены обозначаются как 12.8万, где "万" на китайском языке означает "десять тысяч".
        ch_last_symbol_index = 1  # переменная, которая численно равна этому символу в цене машины
        for sym_id in range(len(parsed_price)):
            if sym_id == 0 and not price_ch[sym_id].isdigit():
                parsed_price = parsed_price[1:]
            elif not price_ch[sym_id].isdigit() and price_ch[sym_id] != '.':
                ch_last_symbol_index = w2n.word_to_num(translate_text(price_ch[sym_id], 'en'))
                parsed_price = parsed_price[:sym_id - 1]
                break

        price_ch = float(parsed_price) * ch_last_symbol_index
        price_ru = float(parsed_price) * ch_last_symbol_index * get_cny_rate()

        #  ПРОИЗВОДИТЕЛЬ И МОДЕЛЬ
        brand_block = soup.find("div", class_="bread-crumbs content")
        brands_info_li = brand_block.find_all('a')

        producer = translate_text(brands_info_li[-3].get_text(), 'en').replace('Second -hand ', '') \
            .replace('Second -hand', '')
        model = translate_text(brands_info_li[-2].get_text(), 'en').replace('Second -hand ', '') \
            .replace('Second -hand', '')

        if not producer:
            print(f'[GET DATA] ({current_process_name}) ERROR! Could\'t get producer.')
            logs.log_warning(f'[GET DATA] ({current_process_name}) ERROR! Could\'t get producer.')
            return None

        # ОПИСАНИЕ
        description_block = soup.find('p', class_='message-box over-hide')
        # если описание не было найдено
        if not description_block:
            print(f"[GET DATA] ({current_process_name}) Couldn't find description block.")
            logs.log_warning(f"[GET DATA] ({current_process_name}) Couldn't find description block.")
            return None
        description_CH = description_block.get_text()
        description_RU = translate_text(description_CH, 'ru')
        description_EN = translate_text(description_CH, 'en')

        descriptions_list = [description_CH, description_EN, description_RU]

        # ДАННЫЕ О ДИЛЕРЕ
        dealer_block = soup.find('a', class_='company-left-link')
        if not dealer_block:
            print(f"[GET DATA] ({current_process_name}) Couldn't find dealer data block.")
            logs.log_warning(f"[GET DATA] ({current_process_name}) Couldn't find dealer data block.")
            return None
        dealer_url = "https://dealers.che168.com/shop" + dealer_block.get('href')
        if dealer_block.find('span', class_='manger-name'):
            dealer_name = (translate_text(dealer_block.find('span', class_='manger-name').get_text(), 'en'))
        else:
            print(f"[GET DATA] ({current_process_name}) Couldn't find dealer name.")
            logs.log_warning(f"[GET DATA] ({current_process_name}) Couldn't find dealer name.")
            return None

        dealer_data = [dealer_name, dealer_url]

        # КАТЕГОРИИ И ОПЦИИ
        # часть информации из карточки товара
        vehicle_card = soup.find('ul', class_='brand-unit-item fn-clear')
        card_info_li = [elem.get_text().strip() for elem in vehicle_card.find_all('h4')]

        # пробег
        parsed_mileage = card_info_li[0]
        for sym_id in range(len(parsed_mileage)):
            if parsed_mileage[sym_id] != '.' and not parsed_mileage[sym_id].isdigit():
                parsed_mileage = float(parsed_mileage[:sym_id]) * \
                                 w2n.word_to_num(translate_text(parsed_mileage[sym_id], 'en'))
                break
        # дата регистрации
        match_date = re.findall(r'\d+', card_info_li[1])
        reg_date = match_date[1] + '/' + match_date[0]
        # передача и объём двигателя
        transvolume_plaintext = translate_text(card_info_li[2], 'ru')
        transmission_match = re.search(r"(\w+) /", transvolume_plaintext)
        volume_match = re.search(r"/ (\w.*)", transvolume_plaintext)

        if transmission_match:
            transmission = transmission_match.group(1)
        else:
            transmission = "Не найдено"

        if volume_match:
            volume = volume_match.group(1)
        else:
            volume = "Не найдено"

        options_dict = {'Дата регистрации': reg_date, 'Пробег': parsed_mileage, 'Передача': transmission,
                        'Объём двигателя': volume}

        # если на странице товара есть кнопка "Больше опций"
        try:
            soup = get_htmlsoup(driver)

            try:
                if soup.find("i", class_="usedfont used-guanbi pricedownclose"):
                    close_trashtab_button = driver.find_element(By.XPATH,
                                                                '/html/body/div[26]/div/div[2]/a[2]')
                    close_trashtab_button.click()

            except selenium.common.exceptions.ElementNotInteractableException as _ex:
                pass
            except Exception as _ex:
                print(_ex)
            # получение идентификатора текущей вкладки
            current_tab = driver.current_window_handle
            button_moreconfig = driver.find_element(By.ID, "a_moreconfig")
            actions.move_to_element(button_moreconfig).perform()
            button_moreconfig.click()
            # переключение на новую вкладку
            for tab in driver.window_handles:
                if tab != current_tab:
                    driver.switch_to.window(tab)

            print(f'[GET DATA] ({current_process_name}) Redirect to moreconfig page.')
            logs.log_info(f'[GET DATA] ({current_process_name}) Redirect to moreconfig page.')
            time.sleep(1)

            # работа с таблицей опций
            soup = get_htmlsoup(driver)
            div_tablesblock = soup.find('div', class_="config-right-con")
            categories_CH_li = [elem.get_text().strip() for elem in div_tablesblock.find_all('td', class_='table-left')]
            values_CH_li = [elem.get_text().strip() for elem in div_tablesblock.find_all('td', class_='table-right')]

            # запись категорий и их значений в строку через разделитель, чтобы далее translate_text() единожды
            parsed_categories_string_CH = ''
            parsed_values_string_CH = ''
            separator = "\n"

            tmp = 1
            for elm in categories_CH_li:
                parsed_categories_string_CH += separator + elm
                tmp += 1
            tmp = 1
            for elm in values_CH_li:
                parsed_values_string_CH += separator + elm
                tmp += 1

            # print(translate_text(parsed_categories_string_CH, 'en'), translate_text(parsed_values_string_CH, 'en'))
            parsed_values_string_CH = parsed_values_string_CH
            parsed_categories_li = translate_text(parsed_categories_string_CH, 'ru').split(separator)
            parsed_values_li = translate_text(parsed_values_string_CH, 'ru').replace('●', 'Есть ').replace('○', 'Нет ')\
                .replace('-', 'Нет ').replace('Никто', 'Нет ').split(separator)

            for elem_id in range(len(parsed_categories_li)):
                options_dict[parsed_categories_li[elem_id].capitalize()] = parsed_values_li[elem_id].capitalize()

        except NoSuchElementException as _ex:
            # если кнопки нет выдёргиваются базовые опции
            print(f'[GET DATA] ({current_process_name}) Collecting options without redirect to moreconfig page.')
            logs.log_info(f'[GET DATA] ({current_process_name}) Collecting options without redirect to moreconfig page.'
                          )

        finally:
            if len(driver.window_handles) >= 2:
                current_tab = driver.current_window_handle
                driver.execute_script('window.close();')
                for tab in driver.window_handles:
                    if tab != current_tab:
                        driver.switch_to.window(tab)

        # ИЗОБРАЖЕНИЯ
        soup = get_htmlsoup(driver)
        raw_img_block = soup.find('div', class_='car-pic-list js-box-text')
        raw_img_li = raw_img_block.find_all('img')
        img_li = []
        for img in raw_img_li:
            if img == 'x.autoimg.cn/2scimg/m/20221226/default-che168.png':
                print("[GET DATA] Images wasn't load correctly...")
                logs.log_warning(f"[GET DATA] Images wasn't load correctly... ({current_process_name}), {url}")
            img_li.append('https:' + img.get('src'))

        # ИТОГОВЫЙ СЛОВАРЬ С ДАННЫМИ ПОЗИЦИИ
        product_data = {'Name': name, 'Producer': producer, 'TMP_model': model, 'PriceRU': price_ru,
                        'PriceCH': price_ch, 'URL': url,
                        'CoverIMG': img_li[0], 'ImgLi': img_li, 'Descriptions': descriptions_list,
                        'DealerData': dealer_data, 'Options': options_dict}
        logs.log_info(f'[GET DATA] Product data has been successfully obtained {product_data}'.encode('utf-8'))

        return product_data

    except Exception as _ex:
        print(f"[GET DATA INFO] ({current_process_name}) An error occured while trying to get data - {_ex}. ")
        logs.log_warning(f"[GET DATA INFO] ({current_process_name}) An error occured while trying to get data - {_ex}.")
        return None


# получает на вход driver с page_source страницы, с которой необходимо выудить все ссылки на товары и записывает
# ссылки в файл
def get_product_links_from_page(driver, products_urls_file: str):
    soup = get_htmlsoup(driver)
    current_process_name = current_process().name

    if not soup:
        return None

    raw_soup_li = soup.find_all('a', class_='carinfo')
    if raw_soup_li:
        with open(os.path.join(BASE_DIR, "data_parser", "links", products_urls_file), "a") as out:
            for idx in range(len(raw_soup_li) - 1):  # -1 т.к. последняя ссылка в массиве - мусор
                link = raw_soup_li[idx].get('href')
                # похоже, что на сайте существует два типа ссылок авто с хостом и без, обработка обоих случаев
                if link[1] == '/':
                    link = 'https:' + link
                else:
                    link = HOST + link
                out.write(link + '\n')

        print(f'[GET PRODUCT LINKS INFO] ({current_process_name}) There was found {len(raw_soup_li)} links.')
        logs.log_info(f'[GET PRODUCT LINKS INFO] ({current_process_name}) There was found {len(raw_soup_li)} links.')

    # переход на следующую страницу
    try:
        new_url = soup.find('a', class_='page-item-next')
        if new_url:
            new_url = new_url.get('href')
            driver.execute_script('window.location.href = arguments[0];', HOST + new_url)
            print(f'[GET PRODUCT LINKS INFO] ({current_process_name}) Transition to new page {HOST + new_url}')
            logs.log_info(f'[GET PRODUCT LINKS INFO] ({current_process_name}) Transition to new page {HOST + new_url}')
            time.sleep(3)
            get_product_links_from_page(driver, products_urls_file)
        else:
            print(f'[GET PRODUCT LINKS INFO] ({current_process_name}) Retryning to get links again '
                  f'from page ({driver.current_url}).')
            logs.log_warning(f'[GET PRODUCT LINKS INFO] ({current_process_name}) Retryning to get links again '
                             f'from page ({driver.current_url}).')

            # вторая попытка получить ссылки на автомобили со страницы
            time.sleep(5)
            driver.refresh()
            new_url = soup.find('a', class_='page-item-next')

            if new_url:
                new_url = new_url.get('href')
                driver.execute_script('window.location.href = arguments[0];', HOST + new_url)
                print(f'[GET PRODUCT LINKS INFO] ({current_process_name}) Transition to new page {HOST + new_url}')
                logs.log_info(
                    f'[GET PRODUCT LINKS INFO] ({current_process_name}) Transition to new page {HOST + new_url}')
                time.sleep(3)
                get_product_links_from_page(driver, products_urls_file)
            else:
                print(f'[GET PRODUCT LINKS INFO] ({current_process_name}) No links were found.')
                logs.log_warning(f'[GET PRODUCT LINKS INFO] ({current_process_name}) No links were found.')
                return None

    except Exception as _ex:
        print(f'[GET PRODUCT LINKS INFO] ({current_process_name}) An error occured while trying to get next page'
              f' - {_ex}.')
        logs.log_warning(f'[GET PRODUCT LINKS INFO] ({current_process_name})'
                         f' An error occured while trying to get next page - {_ex}.')


# запись всех активных ссылок для парсинга в файл
def get_target_urls(categories_urls_file: str, products_urls_file: str):
    # очистка содержимого product_urls_to_parse.txt
    with open(os.path.join(BASE_DIR, "data_parser", "links", products_urls_file), "w"):
        pass

    # создаём и открываем окно браузера
    driver = create_driver()
    driver.get(HOST)
    current_process_name = current_process().name
    with open(os.path.join(BASE_DIR, 'data_parser', 'links', categories_urls_file)) as ca_input:
        for category_link in ca_input:
            try:
                print(f"[GET TARGET URLS INFO] ({current_process_name}) Current parsing category url"
                      f" - {category_link}", end="")
                logs.log_info(f"[GET TARGET URLS INFO] ({current_process_name}) Current parsing category url"
                              f" - {category_link}")
                driver.execute_script('window.location.href = arguments[0];', category_link)
                get_product_links_from_page(driver, products_urls_file)
            except Exception as _ex:
                print(f'[GET TARGET URLS INFO] ({current_process_name}) '
                      f'An error occured while trying to get target urls {_ex}')
                logs.log_warning(f'[GET TARGET URLS INFO] ({current_process_name}) '
                                 f'An error occured while trying to get target urls {_ex}')
                # если ошибочно закрылись все окна браузера или сам браузер был аварийно закрыт, начинаем новую сессию
                if len(driver.window_handles) == 0 or driver is None or driver.service.process is None:
                    print('[PARSING RPOCESS] Recreating driver.')
                    logs.log_info('[PARSING RPOCESS] Recreating driver.')
                    driver = create_driver(images_enabled=True)
                    driver.get('https://duckduckgo.com')
        kill_driver(driver)


# проверка валидности товара по факту переадрессации
def validate_product_activity(url):
    response = requests.get(url)
    # если в конечном адрессе присутствует данная строка, то элемент неактивен
    isRedirectedToErrorPage = 'https://www.che168.com/CarDetail/wrong.aspx' in response.url
    logs.log_info(f"[VALIDATE PRODUCT ACTIVITY] Activity: {not isRedirectedToErrorPage}; URL: {url}; "
                  f"RESPONSE URL: {response.url};")
    return not isRedirectedToErrorPage
