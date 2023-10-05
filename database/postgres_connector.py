import psycopg2
import logs

from database.credentials import host, user, password, db_name


# получение соединения с БД
def get_connection_to_db():
    try:
        # соединение с существующей базой данных
        connection = psycopg2.connect(
            host=host,
            user=user,
            password=password,
            database=db_name
        )

        connection.autocommit = True
        print("[PostGreSQL INFO] Connected successfully.")
        logs.log_info("[PostGreSQL INFO] Connected successfully.")
        return connection
    except Exception as _ex:
        print("[PostGreSQL INFO] Error while working with PostgreSQL", _ex)
        logs.log_error(f"[PostGreSQL INFO] Error while working with PostgreSQL: {_ex}.")
        return None


# выполняет запрос в БД и по умолчанию возвращает результат выполняемого запроса в БД
def execute_querry(querry, data_returned=True):
    # получаем соединение
    connection = get_connection_to_db()

    # если соединение установлено успешно
    if connection:
        with connection.cursor() as cursor:
            result = []

            # выполнение БД запроса
            cursor.execute(querry)

            if data_returned:
                try:
                    for bd_object in cursor.fetchall():
                        result.append(bd_object[0])

                    print("[PostGreSQL INFO] Data was read successfully.")
                    logs.log_info("[PostGreSQL INFO] Data was read successfully.")
                except psycopg2.ProgrammingError:
                    pass

        connection.commit()
        # прикрываем соединение
        connection.close()
        print("[PostGreSQL INFO] Connection closed.")
        logs.log_info("[PostGreSQL INFO] Connection closed.")
        return result
    else:
        print("[PostGreSQL INFO] Error, couldn't get connection...")
        logs.log_error("[PostGreSQL INFO] Error, couldn't get connection...")
        return None


# получение словаря с моделями из БД
def read_models_from_db():
    # получение марок из БД
    producers_li = execute_querry("SELECT producer from vehicles_models;")

    # получение моделей из БД
    models_li = execute_querry("SELECT model from vehicles_models;")

    if producers_li and models_li:
        # инициализация словаря формата - модель: [марка_модели1, марка_модели2, ...]
        producers_models_data = {producer: [] for producer in set(producers_li)}

        # заполнение словаря моделями
        for indx in range(len(producers_li)):
            producers_models_data[producers_li[indx]].append(models_li[indx])

        return producers_models_data
    else:
        print('Error while trying to read models or producer in read_models_from_db().')
        return None


# возвращает элемент array, который встречается раньше всех в string (определение производителя и модели)
def find_first_occurrence(string, array):
    first_index = len(string) + 1
    size = 0
    result = None

    for item in array:
        index = string.find(item)
        if index != -1 and index < first_index:
            first_index = index
            size = len(item)
            result = item
        elif index == first_index and len(item) > size:
            first_index = index
            size = len(item)
            result = item

    return result


# запись данных машины в БД
def write_productdata_to_db(product_data):
    # получаем соединение
    connection = get_connection_to_db()

    # запись в БД
    # если соединение установлено успешно
    if connection:
        with connection.cursor() as cursor:

            # ОСНОВНАЯ ИНФОРМАЦИЯ
            name = product_data['Name']
            url = product_data['URL']
            total_price = product_data['PriceRU']
            price_ch = product_data['PriceCH']

            # ПРОИЗВОДИТЕЛЬ И МОДЕЛЬ
            all_models_dict = read_models_from_db()

            producer = product_data['Producer']
            model = find_first_occurrence(name, all_models_dict.get(producer, []))
            if not model:
                if product_data['TMP_model']:
                    model = product_data['TMP_model']
                else:
                    print("[WRITE DATA TO DB] Couldn't find model.")
                    logs.log_warning(f"[WRITE DATA TO DB] Couldn't find model. ({name})")
                    connection.commit()
                    # прикрываем соединение
                    connection.close()
                    print("[PostGreSQL INFO] Connection closed.")
                    logs.log_info("[PostGreSQL INFO] Connection closed.")
                    return None

            # DEALER DATA
            dealer_data = product_data['DealerData']
            dealer_name = dealer_data[0]
            dealer_url = dealer_data[1]

            # ИЗОБРАЖЕНИЯ
            img1 = img2 = img3 = img4 = img5 = img6 = img7 = img8 = img9 = img10 = img11 = img12 = img13 = img14 \
                = img15 = ''

            if len(product_data['ImgLi']) >= 1:
                img1 = product_data['ImgLi'][0]
            if len(product_data['ImgLi']) >= 2:
                img2 = product_data['ImgLi'][1]
            if len(product_data['ImgLi']) >= 3:
                img3 = product_data['ImgLi'][2]
            if len(product_data['ImgLi']) >= 4:
                img4 = product_data['ImgLi'][3]
            if len(product_data['ImgLi']) >= 5:
                img5 = product_data['ImgLi'][4]
            if len(product_data['ImgLi']) >= 6:
                img6 = product_data['ImgLi'][5]
            if len(product_data['ImgLi']) >= 7:
                img7 = product_data['ImgLi'][6]
            if len(product_data['ImgLi']) >= 8:
                img8 = product_data['ImgLi'][7]
            if len(product_data['ImgLi']) >= 9:
                img9 = product_data['ImgLi'][8]
            if len(product_data['ImgLi']) >= 10:
                img10 = product_data['ImgLi'][9]
            if len(product_data['ImgLi']) >= 11:
                img11 = product_data['ImgLi'][10]
            if len(product_data['ImgLi']) >= 12:
                img12 = product_data['ImgLi'][11]
            if len(product_data['ImgLi']) >= 13:
                img13 = product_data['ImgLi'][12]
            if len(product_data['ImgLi']) >= 14:
                img14 = product_data['ImgLi'][13]
            if len(product_data['ImgLi']) >= 15:
                img15 = product_data['ImgLi'][14]

            # ОПИСАНИЕ
            descriptions_li = product_data['Descriptions']
            description_CH = descriptions_li[0]
            description_EN = descriptions_li[1]
            description_RU = descriptions_li[2]

            # КАТЕГОРИИ
            options_dict = product_data['Options']
            category1 = category2 = category3 = category4 = category5 = category6 = category7 = category8 = \
                category9 = category10 = category11 = category12 = category13 = category14 = category15 = category16 \
                = category17 = category18 = category19 = category20 = category21 = category22 = category23 = category24\
                = category25 = category26 = category27 = category28 = category29 = category30 = category31 = category32\
                = category33 = category34 = category35 = category36 = category37 = category38 = category39 = category40\
                = category41 = category42 = category43 = category44 = category45 = category46 = category47 = category48\
                = category49 = category50 = category51 = category52 = category53 = category54 = category55 = category56\
                = category57 = category58 = ''

            if 'Дата регистрации' in options_dict:
                category1 = options_dict['Дата регистрации']
            if 'Пробег' in options_dict:
                category2 = options_dict['Пробег']
            if 'Передача' in options_dict:
                category3 = options_dict['Передача']
            if 'Объём двигателя' in options_dict:
                category4 = options_dict['Объём двигателя']
            if 'Официальный 0-100 км/ч ускорение (и)' in options_dict:
                category5 = options_dict['Официальный 0-100 км/ч ускорение (и)']
            if 'Функция мониторинга давления в шинах' in options_dict:
                category6 = options_dict['Функция мониторинга давления в шинах']
            if 'Абс' in options_dict:
                category7 = options_dict['Абс']
            if 'Распределение тормозной мощности (ebd/cbc и т. д.)' in options_dict:
                category8 = options_dict['Распределение тормозной мощности (ebd/cbc и т. д.)']
            if 'Помощь тормозам (eba/bas/ba и т. д.)' in options_dict:
                category9 = options_dict['Помощь тормозам (eba/bas/ba и т. д.)']
            if 'Управление направлением (asr/tcs/trc и т. д.)' in options_dict:
                category10 = options_dict['Управление направлением (asr/tcs/trc и т. д.)']
            if 'Контроль устойчивости тела (esc/esp/dsc и т. д.)' in options_dict:
                category11 = options_dict['Контроль устойчивости тела (esc/esp/dsc и т. д.)']
            if 'Система раннего предупреждения о выходе из полосы движения' in options_dict:
                category12 = options_dict['Система раннего предупреждения о выходе из полосы движения']
            if 'Активное торможение/активная система безопасности' in options_dict:
                category13 = options_dict['Активное торможение/активная система безопасности']
            if 'Перед предупреждением о столкновении' in options_dict:
                category14 = options_dict['Перед предупреждением о столкновении']
            if 'Переключатель режима вождения' in options_dict:
                category15 = options_dict['Переключатель режима вождения']
            if 'Технология запуска двигателя' in options_dict:
                category16 = options_dict['Технология запуска двигателя']
            if 'Автоматическая парковка' in options_dict:
                category17 = options_dict['Автоматическая парковка']
            if 'Круизная система' in options_dict:
                category18 = options_dict['Круизная система']
            if 'Спутниковая навигационная система' in options_dict:
                category19 = options_dict['Спутниковая навигационная система']
            if 'Тип окна в крыше' in options_dict:
                category20 = options_dict['Тип окна в крыше']
            if 'Центральный размер экрана управления' in options_dict:
                category21 = options_dict['Центральный размер экрана управления']
            if 'Bluetooth/автомобильный телефон' in options_dict:
                category22 = options_dict['Bluetooth/автомобильный телефон']
            if 'Интеллектуальная система' in options_dict:
                category23 = options_dict['Интеллектуальная система']
            if 'Полный жк -панель приборов' in options_dict:
                category24 = options_dict['Полный жк -панель приборов']
            if 'Жк -инструмент размер' in options_dict:
                category25 = options_dict['Жк -инструмент размер']
            if 'Тип энергии' in options_dict:
                category26 = options_dict['Тип энергии']
            if 'Максимальная мощность (квт)' in options_dict:
                category27 = options_dict['Максимальная мощность (квт)']
            if 'Максимальный крутящий момент (n · м)' in options_dict:
                category28 = options_dict['Максимальный крутящий момент (n · м)']
            if 'Двигатель' in options_dict:
                category29 = options_dict['Двигатель']
            if 'Коробка передач' in options_dict:
                category30 = options_dict['Коробка передач']
            if 'Длинная*ширина*высота (мм)' in options_dict:
                category31 = options_dict['Длинная*ширина*высота (мм)']
            if 'Конструкция кузова' in options_dict:
                category32 = options_dict['Конструкция кузова']
            if 'Большая скорость (км/ч)' in options_dict:
                category33 = options_dict['Большая скорость (км/ч)']
            if 'Официальный 0-100 км/ч ускорение (и)' in options_dict:
                category34 = options_dict['Официальный 0-100 км/ч ускорение (и)']
            if 'Wltc комплексный расход топлива (l/100 км)' in options_dict:
                category35 = options_dict['Wltc комплексный расход топлива (l/100 км)']
            if 'Длина (мм)' in options_dict:
                category36 = options_dict['Длина (мм)']
            if 'Ширина (мм)' in options_dict:
                category37 = options_dict['Ширина (мм)']
            if 'Высота (мм)' in options_dict:
                category38 = options_dict['Высота (мм)']
            if 'Колочная бабара (мм)' in options_dict:
                category39 = options_dict['Колочная бабара (мм)']
            if 'Переднее колесо (мм)' in options_dict:
                category40 = options_dict['Переднее колесо (мм)']
            if 'Расстояние задних колес (мм)' in options_dict:
                category41 = options_dict['Расстояние задних колес (мм)']
            if 'Количество мест (одно)' in options_dict:
                category42 = options_dict['Количество мест (одно)']
            if 'Объем топливного бака (l)' in options_dict:
                category43 = options_dict['Объем топливного бака (l)']
            if 'Масса (кг)' in options_dict:
                category44 = options_dict['Масса (кг)']
            if 'Максимальное качество полной нагрузки (кг)' in options_dict:
                category45 = options_dict['Максимальное качество полной нагрузки (кг)']
            if 'Расположение цилиндра форма' in options_dict:
                category46 = options_dict['Расположение цилиндра форма']
            if 'Номер цилиндра (один)' in options_dict:
                category47 = options_dict['Номер цилиндра (один)']
            if 'Количество клапана на цилиндр (один)' in options_dict:
                category48 = options_dict['Количество клапана на цилиндр (один)']
            if 'Подача воздуха' in options_dict:
                category49 = options_dict['Подача воздуха']
            if 'Максимальная мощность (ps)' in options_dict:
                category50 = options_dict['Максимальная мощность (ps)']
            if 'Максимальная скорость мощности (об / мин)' in options_dict:
                category51 = options_dict['Максимальная скорость мощности (об / мин)']
            if 'Максимальная скорость крутящего момента (обороты)' in options_dict:
                category52 = options_dict['Максимальная скорость крутящего момента (обороты)']
            if 'Максимальная чистая мощность (квт)' in options_dict:
                category53 = options_dict['Максимальная чистая мощность (квт)']
            if 'Форма топлива' in options_dict:
                category54 = options_dict['Форма топлива']
            if 'Топливный ярлык' in options_dict:
                category55 = options_dict['Топливный ярлык']
            if 'Поставка топлива' in options_dict:
                category56 = options_dict['Поставка топлива']
            if 'Количество передач' in options_dict:
                category57 = options_dict['Количество передач']
            if 'Метод привода' in options_dict:
                category58 = options_dict['Метод привода']

            # SQL запрос
            query = "INSERT INTO vehicles_data (name, producer, model, url, price, img1, img2, img3, img4, " \
                    "img5, img6, img7, img8, img9, img10, img11, img12, img13, img14, img15, category1, category2," \
                    " category3, category4, category5, category6, category7, category8, category9, category10," \
                    " category11, category12, category13, category14, category15, category16, category17," \
                    " category18, category19, category20, category21, category22, category23, category24, category25, "\
                    "category26, category27, category28, category29, category30, category31, category32, category33, " \
                    "category34, category35, category36, category37, category38, category39, category40, category41, " \
                    "category42, category43, category44, category45, category46, category47, category48, category49, " \
                    "category50, category51, category52, category53, category54, category55, category56, category57," \
                    " category58, description_ch, description_ru, description_en, dealer_name, dealer_url, price_ch )" \
                    " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s," \
                    "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                    "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                    "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

            cursor.execute(query, (name, producer, model, url, total_price, img1, img2, img3,
                                   img4, img5, img6, img7, img8, img9, img10, img11, img12, img13, img14, img15,
                                   category1, category2, category3, category4, category5, category6, category7,
                                   category8, category9, category10, category11, category12, category13, category14,
                                   category15, category16, category17, category18, category19, category20, category21,
                                   category22, category23, category24, category25, category26, category27, category28,
                                   category29, category30, category31, category32, category33, category34, category35,
                                   category36, category37, category38, category39, category40, category41, category42,
                                   category43, category44, category45, category46, category47, category48, category49,
                                   category50, category51, category52, category53, category54, category55, category56,
                                   category57, category58, description_CH, description_RU, description_EN, dealer_name,
                                   dealer_url, price_ch))
            print("[PostGreSQL INFO] Data was wrote to DataBase.")
            logs.log_info(f"[PostGreSQL INFO] Data was wrote to DataBase. ({name, url})")
        connection.commit()
        # прикрываем соединение
        connection.close()
        print("[PostGreSQL INFO] Connection closed.")
        logs.log_info("[PostGreSQL INFO] Connection closed.")
    else:
        print("[PostGreSQL INFO] Error, couldn't get connection...")
        logs.log_error("[PostGreSQL INFO] Error, couldn't get connection...")
