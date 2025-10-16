import pandas as pd
from pymongo import MongoClient
import matplotlib.pyplot as plt
from datetime import datetime
import pytz
from pathlib import Path
import tkinter as tk
from tkinter import ttk, messagebox
import threading
import requests
from tkcalendar import DateEntry

# Словарь для сопоставления названий городов с их часовыми поясами
CITY_TIMEZONES = {
    'Москва': 'Europe/Moscow',
    'Красноярск': 'Asia/Krasnoyarsk',
    'Новосибирск': 'Asia/Novosibirsk',
    'Екатеринбург': 'Asia/Yekaterinburg',
    'Владивосток': 'Asia/Vladivostok',
    'Калининград': 'Europe/Kaliningrad',
    'Краснодар': 'Europe/Moscow',
    'Санкт-Петербург': 'Europe/Moscow',
    'Иркутск': 'Asia/Irkutsk',
    'Самара': 'Europe/Samara',
}

# --- Функции для работы с данными ---

def get_db_collection(db_name="hh_vacancies_db", collection_name="vacancies"):
    """Создает и возвращает подключение к коллекции MongoDB."""
    try:
        client = MongoClient('mongodb://localhost:27017/', serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        db = client[db_name]
        return db[collection_name]
    except Exception:
        return None

def get_professional_roles():
    """Получает список профессиональных специализаций с API hh.ru и группирует их по категориям."""
    url = "https://api.hh.ru/professional_roles"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        categories = {}
        for category in data.get('categories', []):
            category_name = category.get('name')
            roles = [role.get('name') for role in category.get('roles', [])]
            categories[category_name] = sorted(list(set(roles)))
        return categories
    except requests.exceptions.RequestException as e:
        print(f"Ошибка при получении специализаций с hh.ru: {e}")
        return {}

def create_and_save_plot(df, title, tz, file_prefix):
    """Создает и сохраняет график из DataFrame."""
    plt.style.use('seaborn-v0_8-whitegrid')
    fig, ax = plt.subplots(figsize=(15, 8))
    
    # Определяем цвета для столбцов
    bar_colors = ['red' if dt.hour == 0 else 'dodgerblue' for dt in df['datetime_local']]
    ax.bar(df['datetime_local'], df['total_hourly_increase'], width=0.04, color=bar_colors, zorder=3)

    # Добавляем подписи над столбцами
    for i, bar in enumerate(ax.patches):
        height = bar.get_height()
        ax.annotate(f'{int(height)}',
                    xy=(bar.get_x() + bar.get_width() / 2, height),
                    xytext=(0, 3),
                    textcoords="offset points",
                    ha='center', va='bottom', fontsize=9)

    ax.set_title(title, fontsize=18, fontweight='bold')
    ax.set_ylabel('Суммарный прирост откликов', fontsize=12)
    
    # Настройка меток оси X
    tick_positions = df['datetime_local']
    tick_labels = []
    
    for dt in tick_positions:
        if dt.hour == 0:
            # Для полуночи выводим и время, и дату, но с переносом строки
            tick_labels.append(dt.strftime('%H:%M\n%d.%m.%Y'))
        else:
            # Для остальных часов выводим только время
            tick_labels.append(dt.strftime('%H:%M'))

    ax.set_xticks(tick_positions)
    
    # Устанавливаем метки и настраиваем их
    ax.set_xticklabels(tick_labels, rotation=90, ha='center', fontsize=9, fontweight='bold')
    
    # Устанавливаем отступ для меток, чтобы дата была ниже времени
    for label in ax.get_xticklabels():
        if '\n' in label.get_text():
            label.set_ha('center')
            label.set_y(-0.05)
            
    # Добавляем разделители для каждого нового дня
    unique_dates = df['datetime_local'].dt.date.unique()
    for i in range(1, len(unique_dates)):
        day_start = df[df['datetime_local'].dt.date == unique_dates[i]]['datetime_local'].min()
        ax.axvline(day_start, color='red', linestyle='--', linewidth=1.5, zorder=2)
    
    ax.set_xlabel(f'Дата и время ({tz.zone})', fontsize=12)
    ax.set_ylim(0, df['total_hourly_increase'].max() * 1.2)
    plt.tight_layout()

    output_dir = Path("plots")
    output_dir.mkdir(exist_ok=True)
    file_name = f"{file_prefix}_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.png"
    file_path = output_dir / file_name
    plt.savefig(file_path)
    messagebox.showinfo("Готово", f"График успешно сохранен в файл:\n{file_path}")
    plt.show()

def get_data_for_plot(city_name=None, selected_specializations=None, start_date=None, end_date=None):
    """Извлекает и обрабатывает данные из MongoDB с фильтрацией по городу, специализациям и датам."""
    collection = get_db_collection()
    if collection is None:
        messagebox.showerror("Ошибка", "Не удалось подключиться к MongoDB. Проверьте, запущен ли сервер.")
        return None, None, None, None

    # Фильтр по городу и специализациям для первого этапа
    initial_match_filter = {"response_history.total_responses_metric1_increase": {'$gt': 0}}
    if city_name:
        initial_match_filter['city'] = city_name
    if selected_specializations:
        initial_match_filter['specialization'] = {'$in': selected_specializations}

    # Преобразуем даты для фильтрации
    start_datetime_utc = None
    end_datetime_utc = None
    if start_date and end_date:
        start_datetime_utc = datetime.combine(start_date, datetime.min.time()).replace(tzinfo=pytz.UTC)
        end_datetime_utc = datetime.combine(end_date, datetime.max.time()).replace(tzinfo=pytz.UTC)

    if city_name is None and not selected_specializations:
        tz = pytz.timezone('Europe/Moscow')
        file_prefix = "hourly_responses_plot_all_cities"
        title = 'Совокупный прирост откликов по часам (все города)'
    else:
        local_tz_name = CITY_TIMEZONES.get(city_name, 'Europe/Moscow')
        try:
            tz = pytz.timezone(local_tz_name)
        except pytz.UnknownTimeZoneError:
            tz = pytz.timezone('Europe/Moscow')
        
        city_title = f"({city_name})" if city_name else ""
        specialization_title = f" ({len(selected_specializations)} спец.)" if selected_specializations else ""
        title = f"Совокупный прирост откликов{city_title}{specialization_title}"
        file_prefix = f"hourly_responses_plot_{city_name.lower() if city_name else 'all'}"
        if selected_specializations:
            file_prefix += f"_{len(selected_specializations)}_specializations"

    pipeline = [
        {'$match': initial_match_filter},
        {'$unwind': "$response_history"},
        {'$addFields': {
            'parsing_datetime_local': {
                '$dateFromString': {
                    'dateString': {
                        '$concat': [
                            '$response_history.parsing_date',
                            'T',
                            { '$substr': ['$response_history.parsing_time', 0, 5] }
                        ]
                    },
                    'format': '%d.%m.%YT%H:%M'
                }
            }
        }},
        {'$match': {"response_history.total_responses_metric1_increase": {'$gt': 0}}},
    ]

    # Добавляем фильтр по датам, если они были выбраны
    if start_date and end_date:
        pipeline.append({'$match': {
            'parsing_datetime_local': {
                '$gte': start_datetime_utc,
                '$lte': end_datetime_utc
            }
        }})

    pipeline.append({'$group': {
        '_id': {
            'year': { '$year': "$parsing_datetime_local" },
            'month': { '$month': "$parsing_datetime_local" },
            'day': { '$dayOfMonth': "$parsing_datetime_local" },
            'hour': { '$hour': "$parsing_datetime_local" }
        },
        'total_hourly_increase': {'$sum': "$response_history.total_responses_metric1_increase"}
    }})

    pipeline.append({'$sort': {
        '_id.year': 1, '_id.month': 1, '_id.day': 1, '_id.hour': 1
    }})

    try:
        data = list(collection.aggregate(pipeline))
        if not data:
            messagebox.showinfo("Информация", "В базе данных не найдено данных, соответствующих вашим фильтрам.")
            return None, None, None, None
    except Exception as e:
        messagebox.showerror("Ошибка", f"Ошибка при выполнении агрегации:\n{e}")
        return None, None, None, None

    df = pd.json_normalize(data)
    df = df.rename(columns={'_id.year': 'year', '_id.month': 'month', '_id.day': 'day', '_id.hour': 'hour'})
    df['datetime_utc'] = pd.to_datetime(df[['year', 'month', 'day', 'hour']]).dt.tz_localize('Europe/Moscow')
    df['datetime_local'] = df['datetime_utc'].dt.tz_convert(tz)
    
    return df, title, tz, file_prefix

# --- Графический интерфейс ---

class App:
    def __init__(self, root):
        self.root = root
        self.root.title("Анализ откликов на вакансии")
        self.root.geometry("1000x650")
        self.root.resizable(False, False)

        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)

        style = ttk.Style()
        style.theme_use('vista')
        style.configure('TFrame', background='#f0f0f0')
        style.configure('TLabel', background='#f0f0f0', font=('Arial', 10))
        style.configure('TButton', font=('Arial', 10, 'bold'))
        style.configure('TCombobox', font=('Arial', 10))
        style.configure('TCheckbutton', background='#f0f0f0')
        
        main_frame = ttk.Frame(root, padding="15")
        main_frame.pack(fill='both', expand=True)

        title_label = ttk.Label(main_frame, text="Построение графика откликов", font=('Arial', 16, 'bold'))
        title_label.pack(pady=(0, 15))

        # Фрейм с опциями
        options_frame = ttk.Frame(main_frame)
        options_frame.pack(fill=tk.X, pady=10)

        # Выбор города
        city_frame = ttk.Frame(options_frame)
        city_frame.pack(side='left', padx=10, fill=tk.Y, expand=True)
        ttk.Label(city_frame, text="Выберите город:", font=('Arial', 10, 'bold')).pack(pady=(0, 5))
        
        cities = ["Все города"] + list(CITY_TIMEZONES.keys())
        self.city_combo = ttk.Combobox(city_frame, values=cities, state='readonly', width=25)
        self.city_combo.set('Все города')
        self.city_combo.pack()

        # Выбор диапазона дат
        date_frame = ttk.Frame(options_frame)
        date_frame.pack(side='left', padx=10, fill=tk.Y, expand=True)

        self.all_time_var = tk.BooleanVar(value=True)
        self.all_time_var.trace_add("write", self.toggle_date_entries)
        all_time_checkbox = ttk.Checkbutton(date_frame, text="За весь период", variable=self.all_time_var)
        all_time_checkbox.pack(anchor='w', pady=(0, 5))
        
        ttk.Label(date_frame, text="Начальная дата:", font=('Arial', 10, 'bold')).pack(pady=(0, 5))
        self.start_date_entry = DateEntry(date_frame, width=12, background='darkblue', foreground='white', borderwidth=2, locale='ru_RU')
        self.start_date_entry.pack(pady=(0, 10))

        ttk.Label(date_frame, text="Конечная дата:", font=('Arial', 10, 'bold')).pack(pady=(0, 5))
        self.end_date_entry = DateEntry(date_frame, width=12, background='darkblue', foreground='white', borderwidth=2, locale='ru_RU')
        self.end_date_entry.pack()

        specializations_main_frame = ttk.Frame(options_frame)
        specializations_main_frame.pack(side='left', padx=10, fill=tk.BOTH, expand=True)
        
        # Кнопка "Все специализации"
        all_specs_frame = ttk.Frame(specializations_main_frame)
        all_specs_frame.pack(fill=tk.X, pady=(0, 5))
        ttk.Label(all_specs_frame, text="Категории:", font=('Arial', 10, 'bold')).pack(side='left', padx=(0, 10))
        self.all_specs_button = ttk.Button(all_specs_frame, text="Все специализации", command=self.select_all_specializations)
        self.all_specs_button.pack(side='right')

        categories_frame = ttk.Frame(specializations_main_frame)
        categories_frame.pack(side='left', fill=tk.Y, padx=(0, 10))
        
        canvas_categories = tk.Canvas(categories_frame, borderwidth=0, background="#ffffff", width=250, height=400)
        scrollbar_categories = ttk.Scrollbar(categories_frame, orient="vertical", command=canvas_categories.yview)
        scrollable_frame_categories = ttk.Frame(canvas_categories, style='TFrame')
        scrollable_frame_categories.bind("<Configure>", lambda e: canvas_categories.configure(scrollregion=canvas_categories.bbox("all")))
        canvas_categories.create_window((0, 0), window=scrollable_frame_categories, anchor="nw")
        canvas_categories.configure(yscrollcommand=scrollbar_categories.set)
        canvas_categories.pack(side="left", fill="both", expand=True)
        scrollbar_categories.pack(side="right", fill="y")
        
        self.category_vars = {}
        
        specializations_frame = ttk.Frame(specializations_main_frame)
        specializations_frame.pack(side='left', fill=tk.BOTH, expand=True)
        ttk.Label(specializations_frame, text="Специализации:", font=('Arial', 10, 'bold')).pack(pady=(0, 5))

        canvas_specializations = tk.Canvas(specializations_frame, borderwidth=0, background="#ffffff", width=250, height=400)
        scrollbar_specializations = ttk.Scrollbar(specializations_frame, orient="vertical", command=canvas_specializations.yview)
        self.scrollable_frame_specializations = ttk.Frame(canvas_specializations, style='TFrame')
        self.scrollable_frame_specializations.bind("<Configure>", lambda e: canvas_specializations.configure(scrollregion=canvas_specializations.bbox("all")))
        canvas_specializations.create_window((0, 0), window=self.scrollable_frame_specializations, anchor="nw")
        canvas_specializations.configure(yscrollcommand=scrollbar_specializations.set)
        canvas_specializations.pack(side="left", fill="both", expand=True)
        scrollbar_specializations.pack(side="right", fill="y")
        
        self.specialization_vars = {}
        self.specialization_checkboxes = {}
        self.professional_roles_data = {}
        
        button_frame = ttk.Frame(main_frame)
        button_frame.pack(pady=20)
        
        self.plot_button = ttk.Button(button_frame, text="Построить график", command=self.plot_thread)
        self.plot_button.pack(side='left', padx=10)

        self.status_label = ttk.Label(root, text="Готово", relief=tk.SUNKEN, anchor=tk.W)
        self.status_label.pack(side=tk.BOTTOM, fill=tk.X)

        self.scrollable_frame_categories = scrollable_frame_categories
        self.load_specializations_thread()

    def on_closing(self):
        self.root.destroy()

    def set_status(self, text):
        self.status_label.config(text=text)
        self.root.update_idletasks()

    def load_specializations_thread(self):
        self.set_status("Загрузка специализаций с hh.ru...")
        self.plot_button.config(state=tk.DISABLED)
        threading.Thread(target=self._load_specializations, daemon=True).start()

    def _load_specializations(self):
        self.professional_roles_data = get_professional_roles()
        if not self.professional_roles_data:
            self.set_status("Ошибка загрузки специализаций. Попробуйте позже.")
            return

        for category_name in sorted(self.professional_roles_data.keys()):
            var = tk.BooleanVar()
            var.trace_add("write", lambda name, index, mode, cat=category_name: self.on_category_select(cat))
            chk = ttk.Checkbutton(self.scrollable_frame_categories, text=category_name, variable=var)
            chk.pack(anchor='w')
            self.category_vars[category_name] = var
        
        self.plot_button.config(state=tk.NORMAL)
        self.set_status("Готово")

    def on_category_select(self, category_name):
        is_checked = self.category_vars[category_name].get()
        specializations_in_category = self.professional_roles_data.get(category_name, [])

        if is_checked:
            for spec in specializations_in_category:
                if spec not in self.specialization_vars:
                    var = tk.BooleanVar(value=True)
                    cb_name = var.trace_add("write", lambda name, index, mode, s=spec: self.on_specialization_select(s))
                    chk = ttk.Checkbutton(self.scrollable_frame_specializations, text=spec, variable=var)
                    chk.pack(anchor='w')
                    self.specialization_vars[spec] = var
                    self.specialization_checkboxes[spec] = (chk, cb_name)
        else:
            for spec in specializations_in_category:
                if spec in self.specialization_vars:
                    chk, cb_name = self.specialization_checkboxes[spec]
                    self.specialization_vars[spec].trace_remove("write", cb_name)
                    chk.destroy()
                    del self.specialization_vars[spec]
                    del self.specialization_checkboxes[spec]

    def on_specialization_select(self, specialization_name):
        is_checked = self.specialization_vars[specialization_name].get()
        if not is_checked:
            for category_name, specializations in self.professional_roles_data.items():
                if specialization_name in specializations:
                    other_specializations_checked = any(
                        self.specialization_vars.get(spec, tk.BooleanVar(value=False)).get() 
                        for spec in specializations 
                        if spec != specialization_name
                    )
                    if not other_specializations_checked:
                        self.category_vars[category_name].set(False)
                    return
    
    def select_all_specializations(self):
        """Выбирает все специализации и категории."""
        for spec in list(self.specialization_vars.keys()):
            chk, cb_name = self.specialization_checkboxes[spec]
            self.specialization_vars[spec].trace_remove("write", cb_name)
            chk.destroy()
            del self.specialization_vars[spec]
            del self.specialization_checkboxes[spec]
        
        for category_name in sorted(self.professional_roles_data.keys()):
            self.category_vars[category_name].set(True)
            self.on_category_select(category_name)


    def toggle_date_entries(self, *args):
        """Блокирует/разблокирует поля выбора дат в зависимости от чекбокса 'За весь период'."""
        if self.all_time_var.get():
            self.start_date_entry.configure(state='disabled')
            self.end_date_entry.configure(state='disabled')
        else:
            self.start_date_entry.configure(state='normal')
            self.end_date_entry.configure(state='normal')

    def plot_thread(self):
        city = self.city_combo.get()
        selected_specializations = [spec for spec, var in self.specialization_vars.items() if var.get()]

        if city == "Все города":
            city = None

        if self.all_time_var.get():
            start_date = None
            end_date = None
        else:
            start_date = self.start_date_entry.get_date()
            end_date = self.end_date_entry.get_date()
            if start_date > end_date:
                messagebox.showerror("Ошибка", "Начальная дата не может быть позже конечной.")
                return

        if not city and not selected_specializations and not self.all_time_var.get():
            messagebox.showinfo("Предупреждение", "Пожалуйста, выберите город, специализацию или период для построения графика.")
            return

        self.set_status(f"Загрузка данных...")
        threading.Thread(target=self._process_plot, args=(city, selected_specializations, start_date, end_date), daemon=True).start()
    
    def _process_plot(self, city_name, selected_specializations, start_date, end_date):
        try:
            df, title, tz, file_prefix = get_data_for_plot(city_name, selected_specializations, start_date, end_date)
            if df is not None:
                create_and_save_plot(df, title, tz, file_prefix)
            self.set_status("Готово")
        except Exception as e:
            messagebox.showerror("Критическая ошибка", f"Произошла непредвиденная ошибка:\n{e}")
            self.set_status("Готово")

if __name__ == "__main__":
    root = tk.Tk()
    app = App(root)
    app.toggle_date_entries() # Инициализируем состояние полей
    root.mainloop()