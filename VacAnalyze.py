import pandas as pd
from pymongo import MongoClient
from datetime import datetime
import os

def create_pivot_table(db_name="hh_vacancies_db", collection_name="vacancies"):
    """
    Подключается к MongoDB, извлекает данные и создает кросс-таблицу в pandas.
    """
    try:
        client = MongoClient('mongodb://localhost:27017/')
        db = client[db_name]
        collection = db[collection_name]
        print("Подключение к MongoDB успешно.")
    except Exception as e:
        print(f"Ошибка подключения к MongoDB: {e}")
        return None

    # Агрегационный конвейер для извлечения и форматирования данных
    pipeline = [
        {'$unwind': "$response_history"},
        {'$project': {
            '_id': 0,
            'vacancy_id': "$vacancy_id",
            'job_title': "$job_title",
            'company_name': "$company_name",
            'city': "$city",
            'publication_type': "$publication_type",
            'specialization': "$specialization",
            'creation_datetime': {'$concat': ["$creation_date", " ", "$creation_time"]},
            'parsing_datetime_hour': {'$concat': ["$response_history.parsing_date", " ", { '$substr': ["$response_history.parsing_time", 0, 2] }, ":00"]},
            'total_responses_metric1_increase': "$response_history.total_responses_metric1_increase"
        }},
        {'$group': {
            '_id': {
                'vacancy_id': "$vacancy_id",
                'job_title': "$job_title",
                'company_name': "$company_name",
                'city': "$city",
                'publication_type': "$publication_type",
                'specialization': "$specialization",
                'creation_datetime': "$creation_datetime",
                'parsing_datetime_hour': "$parsing_datetime_hour"
            },
            'hourly_increase': {'$sum': "$total_responses_metric1_increase"}
        }},
        {'$project': {
            '_id': 0,
            'vacancy_id': "$_id.vacancy_id",
            'job_title': "$_id.job_title",
            'company_name': "$_id.company_name",
            'city': "$_id.city",
            'publication_type': "$_id.publication_type",
            'specialization': "$_id.specialization",
            'creation_datetime': "$_id.creation_datetime",
            'parsing_datetime_hour': "$_id.parsing_datetime_hour",
            'hourly_increase': "$hourly_increase"
        }}
    ]

    try:
        data = list(collection.aggregate(pipeline))
        if not data:
            print("В базе данных не найдено данных для анализа.")
            return None
    except Exception as e:
        print(f"Ошибка при выполнении агрегации: {e}")
        return None

    # Создание DataFrame
    df = pd.DataFrame(data)

    # Создание сводной таблицы (Pivot Table)
    pivot_table = df.pivot_table(
        index=['vacancy_id', 'job_title', 'company_name', 'city', 'publication_type', 'specialization', 'creation_datetime'],
        columns='parsing_datetime_hour',
        values='hourly_increase',
        aggfunc='first'
    ).reset_index()

    # Заполняем пропущенные значения нулями
    pivot_table = pivot_table.fillna(0)
    
    # Добавление ссылки на вакансию
    pivot_table['Ссылка на вакансию'] = pivot_table['vacancy_id'].apply(lambda x: f"https://hh.ru/vacancy/{x}")

    # Переименование столбцов для удобства
    pivot_table.columns.name = None
    pivot_table = pivot_table.rename(columns={
        'vacancy_id': 'ID вакансии',
        'job_title': 'Название вакансии',
        'company_name': 'Название компании',
        'city': 'Город',
        'publication_type': 'Тип публикации',
        'specialization': 'Специализация',
        'creation_datetime': 'Дата/время создания'
    })
    
    cols = ['ID вакансии', 'Ссылка на вакансию', 'Название вакансии', 'Название компании', 'Город', 'Тип публикации', 'Специализация', 'Дата/время создания']
    date_cols = [col for col in pivot_table.columns if col not in cols]
    final_cols = cols + date_cols
    
    return pivot_table[final_cols]

if __name__ == "__main__":
    df_result = create_pivot_table()

    if df_result is not None:
        print("\nГотовая кросс-таблица с приростом откликов:\n")
        print(df_result)
        
        # Генерация имени файла с датой и временем
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        file_name = f"vacancy_responses_hourly_analysis_{timestamp}.xlsx"
        
        # Сохранение таблицы в Excel-файл
        try:
            df_result.to_excel(file_name, index=False)
            print(f"\nТаблица успешно сохранена в файл '{file_name}'")
        except Exception as e:
            print(f"\nОшибка при сохранении в Excel: {e}")