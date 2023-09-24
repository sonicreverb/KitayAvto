from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
from bs4 import BeautifulSoup
from googletrans import Translator
from pycbrf import ExchangeRates
from word2number import w2n

import os
import logs
import math
import re

HOST = 'https://www.che168.com'
BASE_DIR = os.path.dirname(os.path.dirname(__file__))


# возвращает пустой драйвер
def create_driver(images_enabled=False):
    # оключение загрузки изображений для оптимизации
    chrome_options = Options()
    if not images_enabled:
        chrome_options.add_argument(f"--blink-settings=imagesEnabled=false")

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
    try:
        soup = get_htmlsoup(driver)
        if not soup:
            return None

        # НАИМЕНОВАНИЕ ТОВАРА
        name = translate_text(soup.find('h3', class_='car-brand-name').get_text(), 'en')

        # ЦЕНА
        price_ch = soup.find('span', class_='price').get_text()
        if not price_ch:
            print(f"[GET DATA] Couldn't get product price ({url}).")
            logs.log_warning(f"[GET DATA] Couldn't get product price ({url}).")
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
        # округление и форматирование цены в РУБ
        price_ru = math.ceil(price_ru / 10000) * 10000

        # МОДЕЛЬ И МАРКА
        # model_make_block = soup.find("div", class_="bread-crumbs content")

        # ИЗОБРАЖЕНИЯ
        raw_img_block = soup.find('div', class_='car-pic-list js-box-text')
        raw_img_li = raw_img_block.find_all('img')
        img_li = []
        for img in raw_img_li:
            img_li.append('https:' + img.get('src'))

        # ОПИСАНИЕ
        description_block = soup.find('p', class_='message-box over-hide')
        # если описание не было найдено
        if not description_block:
            print("[GET DATA] Couldn't find description block.")
            logs.log_warning("[GET DATA] Couldn't find description block.")
            return None
        description_CH = description_block.get_text()
        description_RU = translate_text(description_CH, 'ru')
        description_EN = translate_text(description_CH, 'en')

        descriptions_list = [description_CH, description_EN, description_RU]

        # ДАННЫЕ О ДИЛЕРЕ
        dealer_block = soup.find('a', class_='company-left-link')
        if not dealer_block:
            print("[GET DATA] Couldn't find dealer data block.")
            logs.log_warning("[GET DATA] Couldn't find dealer data block.")
            return None
        dealer_url = "https://dealers.che168.com/shop" + dealer_block.get('href')
        if dealer_block.find('span', class_='manger-name'):
            dealer_name = (translate_text(dealer_block.find('span', class_='manger-name').get_text(), 'en'))
        else:
            print("[GET DATA] Couldn't find dealer name.")
            logs.log_warning("[GET DATA] Couldn't find dealer name.")
            return None

        dealer_data = [dealer_name, dealer_url]
        print(f'DealerData {dealer_data}')

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
            if soup.find("i", class_="usedfont used-guanbi pricedownclose"):
                close_trashtab_button = driver.find_element(By.XPATH,
                                                            '/html/body/div[26]/div/div[2]/a[2]')
                close_trashtab_button.click()
            # получение идентификатора текущей вкладки
            current_tab = driver.current_window_handle
            button_moreconfig = driver.find_element(By.ID, "a_moreconfig")
            button_moreconfig.click()
            # переключение на новую вкладку
            for tab in driver.window_handles:
                if tab != current_tab:
                    driver.switch_to.window(tab)
            # new_tab_page_source = driver.page_source
            # with open('test_page_source.html', 'w', encoding='utf-8') as o:
            #     o.write(new_tab_page_source)
            print('[GET DATA] Redirect to moreconfig page.')
            logs.log_info('[GET DATA] Redirect to moreconfig page.')

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
            print('[GET DATA] Collecting options without redirect to moreconfig page.')
            logs.log_info('[GET DATA] Collecting options without redirect to moreconfig page.')

        # ИТОГОВЫЙ СЛОВАРЬ С ДАННЫМИ ПОЗИЦИИ
        product_data = {'Name': name, 'PriceRU': price_ru, 'PriceCH': price_ch, 'URL': url,
                        'CoverIMG': img_li[0], 'ImgLi': img_li, 'Descriptions': descriptions_list,
                        'DealerData': dealer_data, 'Options': options_dict}
        logs.log_info(f'[GET DATA] Product data has been successfully obtained {product_data}'.encode('utf-8'))

        return product_data

    except Exception as _ex:
        print(f"[GET DATA INFO] An error occured while trying to get data - {_ex}. ")
        logs.log_warning(f"[GET DATA INFO] An error occured while trying to get data - {_ex}. ")
        return None


# получает на вход driver с page_source страницы, с которой необходимо выудить все ссылки на товары и записывает
# ссылки в файл
def get_product_links_from_page(driver):
    soup = get_htmlsoup(driver)
    if not soup:
        return None

    raw_soup_li = soup.find_all('a', class_='carinfo')
    if raw_soup_li:
        with open(os.path.join(BASE_DIR, "data_parser", "links", 'product_urls_to_parse.txt'), "a") as out:
            for idx in range(len(raw_soup_li) - 1):  # -1 т.к. последняя ссылка в массиве - мусор
                link = raw_soup_li[idx].get('href')
                # похоже, что на сайте существует два типа ссылок авто с хостом и без, обработка обоих случаев
                if link[1] == '/':
                    link = 'https:' + link
                else:
                    link = HOST + link
                out.write(link + '\n')

        print(f'[GET PRODUCT LINKS INFO] There was found {len(raw_soup_li)} links.')
        logs.log_info(f'[GET PRODUCT LINKS INFO] There was found {len(raw_soup_li)} links.')

    # переход на следующую страницу
    try:
        new_url = soup.find('a', class_='page-item-next')
        if new_url:
            new_url = new_url.get('href')
            driver.execute_script('window.location.href = arguments[0];', HOST + new_url)
            print(f'[GET PRODUCT LINKS INFO] Transition to new page {HOST + new_url}')
            logs.log_info(f'[GET PRODUCT LINKS INFO] Transition to new page {HOST + new_url}')
            get_product_links_from_page(driver)
        else:
            print('[GET PRODUCT LINKS INFO] No links were found.')
            logs.log_warning('[GET PRODUCT LINKS INFO] No links were found.')
            return None
    except Exception as _ex:
        print(f'[GET PRODUCT LINKS INFO] An error occured while trying to get next page - {_ex}.')
        logs.log_warning(f'[GET PRODUCT LINKS INFO] An error occured while trying to get next page - {_ex}.')


# запись всех активных ссылок для парсинга в файл
def get_target_urls():
    # очистка содержимого product_urls_to_parse.txt
    with open(os.path.join(BASE_DIR, "data_parser", "links", 'product_urls_to_parse.txt'), "w"):
        pass

    # создаём и открываем окно браузера
    driver = create_driver()
    driver.get(HOST)
    with open(os.path.join(BASE_DIR, 'data_parser', 'links', 'categories_urls_to_parse.txt')) as ca_input:
        for category_link in ca_input:
            try:
                print(f"[GET TARGET URLS INFO] Current parsed url - {category_link}", end="")
                logs.log_info(f"[GET TARGET URLS INFO] Current parsed url - {category_link}")
                driver.execute_script('window.location.href = arguments[0];', category_link)
                get_product_links_from_page(driver)
            except Exception as _ex:
                print(f'[GET TARGET URLS INFO] An error occured while trying to get target urls {_ex}')
                logs.log_warning(f'[GET TARGET URLS INFO] An error occured while trying to get target urls {_ex}')
        kill_driver(driver)
