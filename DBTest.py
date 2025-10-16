import requests
import json
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, timedelta
import re
from pymongo import MongoClient
import pytz
from typing import List, Dict, Any

def connect_to_mongodb():
    try:
        client = MongoClient('mongodb://localhost:27017/')
        db = client['hh_vacancies_db']
        collection = db['vacancies']
        print("Подключение к MongoDB успешно.")
        return collection
    except Exception as e:
        print(f"Ошибка подключения к MongoDB: {e}")
        return None

def save_vacancy_to_mongodb(collection, vacancy_data):

    if collection is None:
        return

    moscow_tz = pytz.timezone('Europe/Moscow')
    now_moscow = datetime.now(moscow_tz)

    existing_vacancy = collection.find_one({"vacancy_id": vacancy_data["vacancy_id"]})

    total_responses_increase_1 = 0
    hours_since_last_parsing = 0.0
    responses_metric1_rate = 0.0
    responses_metric2_rate = 0.0

    # Если вакансия уже существует, вычисляем разницу
    if existing_vacancy and "response_history" in existing_vacancy and existing_vacancy["response_history"]:
        last_entry = existing_vacancy["response_history"][-1]
        
        last_responses_1 = last_entry.get("total_responses_metric1", 0)

        if "parsing_datetime_moscow" in last_entry:
            last_parsing_datetime = last_entry.get("parsing_datetime_moscow")
            if last_parsing_datetime and last_parsing_datetime.tzinfo is None:
                last_parsing_datetime = pytz.utc.localize(last_parsing_datetime).astimezone(moscow_tz)
        elif "parsing_date" in last_entry and "parsing_time" in last_entry:
            last_date_str = last_entry.get("parsing_date")
            last_time_str = last_entry.get("parsing_time")
            try:
                last_parsing_datetime = moscow_tz.localize(datetime.strptime(f"{last_date_str} {last_time_str}", "%d.%m.%Y %H:%M:%S"))
            except (ValueError, TypeError):
                last_parsing_datetime = None
        else:
            last_parsing_datetime = None

        current_responses_1 = vacancy_data.get('О(общ)', 0)

        total_responses_increase_1 = current_responses_1 - last_responses_1
        
        if last_parsing_datetime:
            time_difference = now_moscow - last_parsing_datetime
            hours_since_last_parsing = round(time_difference.total_seconds() / 3600, 2)
            
            if hours_since_last_parsing > 0:
                responses_metric1_rate = round(total_responses_increase_1 / hours_since_last_parsing, 2)

    response_entry = {
        "parsing_date": now_moscow.strftime("%d.%m.%Y"),
        "parsing_time": now_moscow.strftime("%H:%M:%S"),
        "total_responses_metric1": vacancy_data.get('О(общ)', 0),
        "total_responses_metric1_increase": total_responses_increase_1,
        "hours_since_last_parsing": hours_since_last_parsing,
        "responses_metric1_rate": responses_metric1_rate
    }
    
    if existing_vacancy and "response_history" in existing_vacancy and existing_vacancy["response_history"]:
        last_entry = existing_vacancy["response_history"][-1]
        last_parsing_datetime_str = f"{last_entry.get('parsing_date')} {last_entry.get('parsing_time')}"
    
        try:
            moscow_tz = pytz.timezone('Europe/Moscow')
            last_parsing_datetime = moscow_tz.localize(datetime.strptime(last_parsing_datetime_str, "%d.%m.%Y %H:%M:%S"))
            now_moscow = datetime.now(moscow_tz)
        
            # Проверяем, прошло ли меньше 60 секунд с момента последней записи
            if (now_moscow - last_parsing_datetime) < timedelta(seconds=60):
             print(f"Для вакансии {vacancy_data['vacancy_id']} новая запись не будет добавлена, так как последняя запись сделана менее минуты назад.")
             return
        except (ValueError, TypeError):
            pass

    try:
        collection.update_one(
            {"vacancy_id": vacancy_data["vacancy_id"]},
            {
                "$push": {"response_history": response_entry},
                "$set": {
                    "job_title": vacancy_data.get('Вакансия'),
                    "company_name": vacancy_data.get('Компания'),
                    "city": vacancy_data.get('Город '),
                    "work_experience": vacancy_data.get('Опыт работы'),
                    "publication_type": vacancy_data.get('Тип публикации'),
                    "is_adv": vacancy_data.get('isAdv') == 'Да',
                    "hh_auction": vacancy_data.get('HH_AUCTION') == 'Да',
                    "compensation_from": vacancy_data.get('ЗП От'),
                    "compensation_to": vacancy_data.get('ЗП До'),
                    "creation_date": vacancy_data.get('Дата соз'),
                    "creation_time": vacancy_data.get('Время с'),
                    "specialization": vacancy_data.get('Специализация'),
                    "publication_date": vacancy_data.get('Дата пуб'),
                    "publication_time": vacancy_data.get('Время п'),
                }
            },
            upsert=True
        )
        print(f"Обновлена запись для вакансии {vacancy_data['vacancy_id']}")
    except Exception as e:
        print(f"Ошибка при сохранении вакансии {vacancy_data.get('vacancy_id')}: {e}")

def find_fastest_growing_vacancies(collection: MongoClient, top_n: int = 5):
    """
    Находит топ N вакансий с самым большим темпом прироста откликов.
    :param collection: Объект коллекции MongoDB.
    :param top_n: Количество вакансий в топе.
    """
    if collection is None:
        return
        
    print(f"\n--- Топ-{top_n} самых быстрорастущих вакансий по темпу прироста откликов (за последние 24 часа) ---")

    moscow_tz = pytz.timezone('Europe/Moscow')
    now_moscow = datetime.now(moscow_tz)
    
    one_day_ago = now_moscow - timedelta(hours=24)

    pipeline = [
        {"$unwind": "$response_history"},
        {"$addFields": {
            "full_datetime": {
                "$dateFromString": {
                    "dateString": {"$concat": [{"$toString": "$response_history.parsing_date"}, " ", {"$toString": "$response_history.parsing_time"}]},
                    "format": "%d.%m.%Y %H:%M:%S",
                    "timezone": "Europe/Moscow"
                }
            }
        }},
        {"$match": {"full_datetime": {"$gte": one_day_ago}}},
        {"$sort": {"full_datetime": -1}},
        {"$group": {
            "_id": "$vacancy_id",
            "latest_entry": {"$first": "$response_history"},
            "job_title": {"$first": "$job_title"},
            "company_name": {"$first": "$company_name"},
            "vacancy_link": {"$first": {"$concat": ["https://hh.ru/vacancy/", "$vacancy_id"]}}
        }},
        {"$sort": {"latest_entry.responses_metric1_rate": -1}},
        {"$limit": top_n},
    ]

    results = list(collection.aggregate(pipeline))

    if not results:
        print("Данных за последние 24 часа не найдено.")
        return

    for result in results:
        latest = result["latest_entry"]
        rate_1 = latest.get("responses_metric1_rate", 0)
        increase_1 = latest.get("total_responses_metric1_increase", 0)
        hours = latest.get("hours_since_last_parsing", 0)
        
        print(f"----------------------------------------")
        print(f"Вакансия: {result['job_title']} (ID: {result['_id']})")
        print(f"Компания: {result['company_name']}")
        print(f"Ссылка: {result['vacancy_link']}")
        print(f"Прошло часов: {hours}")
        print(f"Прирост (метрика 1): {increase_1} откликов (темп: {rate_1} откл/час)")


def calculate_hourly_total_increase(collection: MongoClient):
    """
    Рассчитывает совокупный прирост откликов по всем вакансиям за каждый час.
    :param collection: Объект коллекции MongoDB.
    """
    if collection is None:
        return

    print("\n--- Совокупный прирост откликов по всем вакансиям за каждый час ---")

    pipeline = [
        {"$unwind": "$response_history"},
        {"$group": {
            "_id": {
                "date": "$response_history.parsing_date",
                "hour": {"$substr": ["$response_history.parsing_time", 0, 2]}
            },
            "total_increase_metric1": {"$sum": "$response_history.total_responses_metric1_increase"}
        }},
        {"$sort": {"_id.date": 1, "_id.hour": 1}}
    ]

    results = list(collection.aggregate(pipeline))

    if not results:
        print("Данные для анализа прироста не найдены.")
        return

    for result in results:
        date = result['_id']['date']
        hour = result['_id']['hour']
        total_1 = result['total_increase_metric1']
        
        print(f"Время: {date} {hour}:00 - {int(hour)+1}:00 | Прирост: {total_1} (метрика 1)")

def format_date_time_separate(iso_date_string):
    if iso_date_string is None or iso_date_string == "Не найдено" or iso_date_string == "Автообновление не настроено":
        return "", ""
    try:
        if '.' in iso_date_string:
            iso_date_string = iso_date_string.split('.')[0]
        if '+' in iso_date_string:
            iso_date_string = iso_date_string.split('+')[0]
        elif 'Z' in iso_date_string:
            iso_date_string = iso_date_string.replace('Z', '')
        
        dt_object = datetime.strptime(iso_date_string, "%Y-%m-%dT%H:%M:%S")
        
        return dt_object.strftime("%d.%m.%Y"), dt_object.strftime("%H:%M:%S")
    except ValueError:
        print(f"Предупреждение: Не удалось распарсить дату/время '{iso_date_string}'.")
        return iso_date_string, ""

def fetch_specializations_from_api(api_url: str) -> dict:
    print(f"Получение справочника специализаций с {api_url}...")
    try:
        response = requests.get(api_url, timeout=10)
        response.raise_for_status()
        data = response.json()
        print("Справочник специализаций успешно получен.")
        return data
    except requests.exceptions.RequestException as e:
        print(f"Ошибка при получении справочника специализаций с API: {e}")
        return {}
    except json.JSONDecodeError as e:
        print(f"Ошибка декодирования JSON при получении справочника специализаций: {e}")
        return {}

def create_specialization_lookup_table(json_data: dict) -> dict:
    lookup_table = {}
    if "categories" in json_data and isinstance(json_data["categories"], list):
        for category in json_data["categories"]:
            if "roles" in category and isinstance(category["roles"], list):
                for role in category["roles"]:
                    if "id" in role and "name" in role:
                        lookup_table[str(role["id"])] = role["name"]
    return lookup_table

def parse_hh_vacancies(base_url: str, max_pages: int = 0, specialization_lookup: dict = None) -> list[dict]:
    all_vacancies_data = []
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    vacancy_counter = 0
    current_page = 0
    if "&items_on_page=" not in base_url:
        base_url += "&items_on_page=100"
    else:
        base_url = re.sub(r"&items_on_page=\d+", "&items_on_page=100", base_url)
    while True:
        if max_pages > 0 and current_page >= max_pages:
            print(f"Достигнуто максимальное количество страниц ({max_pages}). Завершение парсинга.")
            break
        page_url = f"{base_url}&page={current_page}"
        print(f"Загрузка страницы {current_page + 1}: {page_url}")
        try:
            response = requests.get(page_url, headers=headers, timeout=15)
            response.raise_for_status()
            html_content = response.text
            soup = BeautifulSoup(html_content, 'html.parser')
            initial_state_tag = soup.find('template', id='HH-Lux-InitialState')
            if not initial_state_tag:
                print(f"Ошибка: Тег <template id='HH-Lux-InitialState'> не найден на странице {current_page + 1}.")
                if current_page == 0:
                    return []
                break
            json_data_str = initial_state_tag.string
            if not json_data_str:
                print(
                    f"Ошибка: JSON-строка внутри тега <template id='HH-Lux-InitialState'> пуста на странице {current_page + 1}.")
                if current_page == 0:
                    return []
                break
            data = json.loads(json_data_str)
            current_page_vacancies = []
            if "vacancySearchResult" in data and isinstance(data["vacancySearchResult"], dict):
                if "vacancies" in data["vacancySearchResult"] and \
                        isinstance(data["vacancySearchResult"]["vacancies"], list):
                    current_page_vacancies = data["vacancySearchResult"]["vacancies"]
                else:
                    print(
                        f"В 'vacancySearchResult' не найден массив 'vacancies' или он имеет неверный формат на странице {current_page + 1}.")
            else:
                print(
                    f"В JSON-данных не найден ключ 'vacancySearchResult' или он имеет неверный формат на странице {current_page + 1}.")
            if not current_page_vacancies:
                print(f"На странице {current_page + 1} вакансий не найдено. Завершение парсинга.")
                break
            for vacancy in current_page_vacancies:
                vacancy_counter += 1
                city = vacancy.get("area", {}).get("name")
                job_title = vacancy.get("name")
                company_name = vacancy.get("company", {}).get("name")
                compencation_mode = vacancy.get("compensation", {}).get("mode")
                compencation_currencyCode = vacancy.get("compensation", {}).get("currencyCode")
                if compencation_mode == 'MONTH' and compencation_currencyCode == 'RUR':
                    compensation_from = vacancy.get("compensation", {}).get("from")
                    compensation_to = vacancy.get("compensation", {}).get("to")
                else:
                    compensation_from = None
                    compensation_to = None
                work_experience = vacancy.get("workExperience")
                vacancy_id = vacancy.get("vacancyId")
                vacancy_link = f"https://hh.ru/vacancy/{vacancy_id}" if vacancy_id else None
                publication_type = vacancy.get("metallic")
                creation_time_raw = vacancy.get("creationTime")
                creation_date_formatted, creation_time_formatted = format_date_time_separate(creation_time_raw)
                publication_date_obj = vacancy.get("publicationTime", {})
                publication_date_raw = publication_date_obj.get("$") if publication_date_obj else None
                publication_date_formatted, publication_time_formatted = format_date_time_separate(publication_date_raw)
                total_responses_count = vacancy.get("totalResponsesCount")
                responses_count = vacancy.get("responsesCount")
                is_adv = vacancy.get("@isAdv", False)
                has_hh_auction = False
                has_zp_promo = False
                click_url = vacancy.get("clickUrl", None)
                vacancy_properties = vacancy.get("vacancyProperties", {})
                if "properties" in vacancy_properties and isinstance(vacancy_properties["properties"], list):
                    for prop_group in vacancy_properties["properties"]:
                        if "property" in prop_group and isinstance(prop_group["property"], list):
                            for prop in prop_group["property"]:
                                if prop.get("propertyType") == "HH_AUCTION":
                                    has_hh_auction = True
                                if prop.get("propertyType") == "ZP_PROMO":
                                    has_zp_promo = True
                professional_role_id = None
                professional_role_name = "Неизвестно"
                if "professionalRoleIds" in vacancy and isinstance(vacancy["professionalRoleIds"], list) and \
                        len(vacancy["professionalRoleIds"]) > 0 and \
                        "professionalRoleId" in vacancy["professionalRoleIds"][0] and \
                        isinstance(vacancy["professionalRoleIds"][0]["professionalRoleId"], list) and \
                        len(vacancy["professionalRoleIds"][0]["professionalRoleId"]) > 0:
                    professional_role_id = str(vacancy["professionalRoleIds"][0]["professionalRoleId"][0])
                    if specialization_lookup and professional_role_id in specialization_lookup:
                        professional_role_name = specialization_lookup[professional_role_id]
                    else:
                        professional_role_name = f"ID: {professional_role_id}"
                hours_since_creation = None
                if creation_date_formatted and creation_time_formatted:
                    try:
                        creation_datetime_str = f"{creation_date_formatted} {creation_time_formatted}"
                        creation_datetime_obj = datetime.strptime(creation_datetime_str, "%d.%m.%Y %H:%M:%S")
                        current_datetime = datetime.now()
                        time_difference = current_datetime - creation_datetime_obj
                        hours_since_creation = round((time_difference.total_seconds() / 3600) / 24, 2)
                    except ValueError:
                        hours_since_creation = "Ошибка даты/времени"
                
                all_vacancies_data.append({
                    "vacancy_id": vacancy_id,
                    "№ ": vacancy_counter,
                    "Город ": city,
                    "Вакансия": job_title,
                    "Опыт работы": work_experience,
                    "Компания": company_name,
                    "Ссылка": vacancy_link,
                    "Тип публикации": publication_type,
                    "isAdv": "Да" if is_adv else "Нет",
                    "HH_AUCTION": "Да" if has_hh_auction else "Нет",
                    "ЗП От": compensation_from,
                    "ЗП До": compensation_to,
                    "Дата соз": creation_date_formatted,
                    "Время с": creation_time_formatted,
                    "Дата пуб": publication_date_formatted,
                    "Время п": publication_time_formatted,
                    "О(общ)": total_responses_count,
                    "О(2)": responses_count,
                    "Специализация": professional_role_name,
                    "Дней Прошло": hours_since_creation
                })
            current_page += 1
        except requests.exceptions.Timeout:
            print(f"Ошибка: Превышено время ожидания запроса для URL: {page_url}. Попробуйте увеличить timeout.")
            break
        except requests.exceptions.RequestException as e:
            print(f"Ошибка при запросе к URL {page_url}: {e}. Пропуск страницы.")
            current_page += 1
        except json.JSONDecodeError as e:
            print(f"Ошибка при декодировании JSON на странице {current_page + 1}: {e}. Пропуск страницы.")
            current_page += 1
        except Exception as e:
            print(f"Произошла непредвиденная ошибка на странице {current_page + 1}: {e}. Пропуск страницы.")
            current_page += 1
    return all_vacancies_data

if __name__ == "__main__":
    specialization_api_url = "https://api.hh.ru/professional_roles"
    specialization_json_data = fetch_specializations_from_api(specialization_api_url)
    specializations_map = {}
    if specialization_json_data:
        specializations_map = create_specialization_lookup_table(specialization_json_data)
    else:
        print("Не удалось получить справочник специализаций с API. Названия специализаций будут отображаться как 'ID: [числовой ID]'.")

    # 1. Подключаемся к базе данных
    vacancies_collection = connect_to_mongodb()
    if vacancies_collection is not None:
        # 2. Получаем данные для парсинга
        base_url_input = input("Введите базовый URL для парсинга: ")
        num_pages_input = input("Введите количество страниц для парсинга (0 - все страницы): ")
        try:
            num_pages_to_parse = int(num_pages_input)
        except ValueError:
            print("Неверный ввод. Будут парсированы все страницы.")
            num_pages_to_parse = 0

        # 3. Парсим вакансии
        print("\nНачинаем парсинг вакансий...")
        all_extracted_data = parse_hh_vacancies(base_url_input, num_pages_to_parse, specializations_map)
        print(f"Парсинг завершен. Найдено {len(all_extracted_data)} вакансий.")

        # 4. Сохраняем данные в MongoDB
        for vacancy in all_extracted_data:
            save_vacancy_to_mongodb(vacancies_collection, vacancy)
        print("\nВсе данные успешно сохранены в MongoDB.")
    else:
        print("\nСкрипт не может продолжить работу без подключения к MongoDB.")