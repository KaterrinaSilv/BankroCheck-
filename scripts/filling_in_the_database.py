import io
import json
import os

import pandas as pd
import psycopg2
from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build
from pdf2image import convert_from_path

load_dotenv()

# Устанавливаем путь для poppler
poppler_path = r'C:\Users\kater\Desktop\колыбель для кошки\банкротство\Release-24.08.0-0\poppler-24.08.0\Library\bin'
os.environ["PATH"] += os.pathsep + poppler_path

# Учетные данные для Google API
SERVICE_ACCOUNT_FILE = '../service_account.json'
SCOPES = ['https://www.googleapis.com/auth/drive']

credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES)

# Инициализация сервиса Google Drive API
service = build('drive', 'v3', credentials=credentials)


def connect_db():
    return psycopg2.connect(
        dbname="Citizen_documents_prototype_v1",
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host="localhost",  # Или IP адрес вашего сервера
        port="5432"  # Порт по умолчанию для PostgreSQL
    )


# Функция для загрузки документа и конвертации в PDF
def download_doc_as_pdf(file_id, output_pdf_path):
    request = service.files().export_media(fileId=file_id, mimeType='application/pdf')
    with io.FileIO(output_pdf_path, 'wb') as pdf_file:
        pdf_file.write(request.execute())


# Функция для конвертации PDF в PNG
def pdf_to_png(pdf_path, output_folder, citizen_id):
    os.makedirs(output_folder, exist_ok=True)
    pages = convert_from_path(pdf_path)
    for page_number, page in enumerate(pages, start=1):
        page.save(f"{output_folder}/{citizen_id}_page_{page_number}.png", "PNG")
    print(f"PDF {pdf_path} успешно конвертирован в PNG и сохранен в {output_folder}.")


# Функция для записи данных в базу данных
def insert_data_to_db(citizen_data, citizen_id, pdf_path, output_folder):
    try:
        conn = connect_db()
        cursor = conn.cursor()

        cursor.execute("INSERT INTO citizens (citizen_id) VALUES (%s) RETURNING citizen_id;", (citizen_id,))
        citizen_id_db = cursor.fetchone()[0]  # Получаем ID нового гражданина

        cursor.execute(
            "INSERT INTO documents (json_data, document_type, citizen_id) VALUES (%s, %s, %s) RETURNING document_id;",
            (json.dumps(citizen_data), 'inventory of property', citizen_id_db))
        document_id_db = cursor.fetchone()[0]  # Получаем ID нового документа

        page_number = 1  # Начальное значение номера страницы
        while True:
            scan_file_path = f"{output_folder}/{citizen_id}_page_{page_number}.png"

            if os.path.isfile(scan_file_path):
                print(f"Файл {scan_file_path} существует. Выполняем код.")

                with open(scan_file_path, 'rb') as scan_file:
                    scan_data = scan_file.read()

                cursor.execute(
                    "INSERT INTO document_scans (document_id, page_number, scan_file) VALUES (%s, %s, %s);",
                    (document_id_db, page_number, scan_data)
                )
                page_number += 1
            else:
                print(f"Файл {scan_file_path} не найден. Выходим из цикла.")
                break  # Выход из цикла, если файл не найден

        conn.commit()
        cursor.close()
        conn.close()
        print(f"Данные о гражданине {citizen_id} успешно загружены в базу данных.")
    except psycopg2.errors.UniqueViolation:
        print(f"Гражданин с ID {citizen_id} уже существует. Пропускаем вставку.")
    except Exception as e:
        print(f"Произошла ошибка: {e}")


# Путь к файлу с данными о гражданах
input_file = '../data/raw/dataset.csv'
output_dir = '../data/processed/citizens_json'
pdf_path = 'document.pdf'  # Временный PDF
scans_output_folder = 'scans_png'

# Создаем папку для JSON-файлов, если её нет
os.makedirs(output_dir, exist_ok=True)
os.makedirs(scans_output_folder, exist_ok=True)

# Загружаем данные из CSV
data = pd.read_csv(input_file)

# Преобразуем данные каждого гражданина в JSON
for index, row in data.iterrows():
    citizen_data = row.to_dict()
    user_id = row['ID']

    json_file_name = f"{user_id}.json"
    json_file_path = os.path.join(output_dir, json_file_name)

    with open(json_file_path, 'w', encoding='utf-8') as f:
        json.dump(citizen_data, f, ensure_ascii=False, indent=4)

    print(f"Создан файл: {json_file_name}")

    file_url = row['Ссылка на документ']

    try:
        file_id = file_url.split('/')[5]
    except IndexError:
        print(f"Строка {index + 1} пропущена: неверный формат ссылки на файл.")
        continue

    # Скачиваем документ и конвертируем его
    download_doc_as_pdf(file_id, pdf_path)
    pdf_to_png(pdf_path, scans_output_folder, citizen_id=user_id)

    # Записываем данные в базу данных
    insert_data_to_db(citizen_data, user_id, pdf_path, scans_output_folder)

    # Удаление PDF после конвертации
    os.remove(pdf_path)

data.to_csv(input_file, index=False)
