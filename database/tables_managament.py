import os.path
import database.postgres_connector as db
import ftplib
import openpyxl

import logs
from data_parser import get_cny_rate, BASE_DIR
from openpyxl.utils import get_column_letter


# запись данных из БД в xlsx файл querry - запрос в БД, filename - название файла в который запись
def write_data_to_xlsx(querry, filename):
    # получаем соединение
    connection = db.get_connection_to_db()

    cny_rate = get_cny_rate()

    # если соединение установлено успешно
    if connection:
        with connection.cursor() as cursor:
            # SQL запрос
            cursor.execute(querry)
            rows = cursor.fetchall()

            # создание XLSX-файла
            workbook = openpyxl.Workbook()
            worksheet = workbook.active

            # запись заголовков столбцов
            columns = [i[0] for i in cursor.description]
            for column_index, column_name in enumerate(columns, start=1):
                column_letter = get_column_letter(column_index)
                worksheet[f"{column_letter}1"] = column_name
                worksheet["BF1"] = 'CNY_rate'

            # запись данных
            for row_index, row in enumerate(rows, start=2):
                for column_index, cell_value in enumerate(row):
                    column_letter = get_column_letter(column_index + 1)
                    try:
                        worksheet[f"{column_letter}{row_index}"] = str(cell_value)
                    except Exception as _ex:
                        print('[XLSX FILE] could\'t write cell value, string error.', _ex)
                        logs.log_warning('[XLSX FILE] could\'t write cell value, string error.', _ex)
                    worksheet[f"BF{row_index}"] = str(cny_rate)

            # сохранение файла
            filename_path = os.path.join(BASE_DIR, 'database', 'output', filename)
            workbook.save(filename_path)

        connection.commit()
        # прикрываем соединение
        connection.close()
        print("[PostGreSQL INFO] Connection closed.")
        print("[XLSX FILE] XLSX file was updated successfully.")
        logs.log_info("[XLSX FILE] XLSX file was updated successfully.")
    else:
        print("[PostGreSQL INFO] Error, couldn't get connection...")


def upload_file_to_ftp(filename):
    ftp_server = 'ivvsavqz.beget.tech'
    ftp_username = 'ivvsavqz_3'
    ftp_password = 'O96QOw&y'
    remote_file_path = filename
    local_file_path = os.path.join(BASE_DIR, 'database', 'output', remote_file_path)

    # подключение к FTP серверу
    with ftplib.FTP(ftp_server, ftp_username, ftp_password) as ftp:
        print('[FTP INFO] Connecting to FTP server...')
        logs.log_info('[FTP INFO] Connecting to FTP server...')
        # открытие файла для чтения
        with open(local_file_path, 'rb') as file:
            # загрузка файла на сервер
            ftp.storbinary(f'STOR {remote_file_path}', file)
            print('[FTP INFO] Result table was uploaded successfully!')
            logs.log_info('[FTP INFO] Result table was uploaded successfully!')
