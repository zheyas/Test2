import os
import yaml
import logging.config
import pandas as pd
from pyspark.sql import SparkSession
from tkinter import Tk, Text, Scrollbar, Button, END, filedialog, simpledialog

from src.etl.extract import extract_data
from etl.transform import transform_data
# Устанавливаем базовый путь к проекту, используя текущий файл
base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Инициализация SparkSession
spark = SparkSession.builder \
    .appName("ProductCategoryApp") \
    .getOrCreate()

# Путь к конфигурационному файлу
config_path = os.path.join(base_path, "config/config.yaml")
if not os.path.isfile(config_path):
    raise FileNotFoundError(f"Configuration file not found: {config_path}")

with open(config_path, 'r') as config_file:
    config = yaml.safe_load(config_file)

# Путь для логов
log_path = os.path.join(base_path, "logs/spark_logs.log")

# Конфигурация логирования
logging_config = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'simpleFormatter': {
            'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        },
    },
    'handlers': {
        'fileHandler': {
            'class': 'logging.FileHandler',
            'formatter': 'simpleFormatter',
            'level': 'INFO',
            'filename': log_path,
        },
    },
    'root': {
        'level': 'INFO',
        'handlers': ['fileHandler'],
    },
}

logging.config.dictConfig(logging_config)
logger = logging.getLogger(__name__)


def edit_csv_file(file_path):
    """Редактирование CSV файла"""
    df = pd.read_csv(file_path)
    column_names = list(df.columns)

    def save_edited_file():
        new_path = filedialog.asksaveasfilename(defaultextension=".csv", filetypes=[("CSV Files", "*.csv")])
        if new_path:
            df.to_csv(new_path, index=False)

    root = Tk()
    root.title(f"Редактирование {file_path}")

    text_widget = Text(root, wrap='word', height=30, width=100)
    text_widget.grid(row=0, column=0, columnspan=4, sticky="nsew")

    # Заполнение текстового виджета данными из CSV
    text_widget.insert(END, df.to_string())

    # Добавление кнопок редактирования
    def add_row():
        """Добавление новой строки"""
        values = []
        for column in column_names:
            value = simpledialog.askstring("Ввод данных", f"Введите значение для {column}:")
            values.append(value)
        df.loc[len(df.index)] = values
        text_widget.delete(1.0, END)
        text_widget.insert(END, df.to_string())

    add_row_btn = Button(root, text="Добавить строку", command=add_row)
    add_row_btn.grid(row=1, column=0, pady=10)

    remove_row_btn = Button(root, text="Удалить строку", command=lambda: remove_row())
    remove_row_btn.grid(row=1, column=1, pady=10)

    save_btn = Button(root, text="Сохранить изменений", command=save_edited_file)
    save_btn.grid(row=1, column=2, pady=10)

    def remove_row():
        index = simpledialog.askinteger("Удаление строки", "Введите индекс строки для удаления:")
        if index is not None and 0 <= index < len(df.index):
            df.drop(df.index[index], inplace=True)
            text_widget.delete(1.0, END)
            text_widget.insert(END, df.to_string())

    root.mainloop()

def display_results(product_category_pairs_df, products_without_categories_df):
    root = Tk()
    root.title("Результаты анализа продуктов и категорий")

    text_widget = Text(root, wrap='word', height=30, width=100)
    text_widget.grid(row=0, column=0, columnspan=3, sticky="nsew")

    scrollbar = Scrollbar(root, command=text_widget.yview)
    scrollbar.grid(row=0, column=3, sticky="ns")

    text_widget['yscrollcommand'] = scrollbar.set

    text_widget.insert(END, "Пары 'Имя продукта – Имя категории':\n")
    for row in product_category_pairs_df.collect():
        text_widget.insert(END, f"{row['product_name']} - {row['category_name']}\n")

    text_widget.insert(END, "\nПродукты без категорий:\n")
    if products_without_categories_df.count() == 0:
        text_widget.insert(END, "Нет продуктов без категории\n")
    else:
        for row in products_without_categories_df.collect():
            text_widget.insert(END, f"{row['product_name']}\n")

    def save_text_to_file():
        save_to_file(text_widget.get("1.0", END))

    def save_to_file(text):
        file_path = filedialog.asksaveasfilename(defaultextension=".txt", filetypes=[("Text Files", "*.txt"), ("All Files", "*.*")])
        if file_path:
            with open(file_path, 'w', encoding='utf-8') as file:
                file.write(text)

    save_button = Button(root, text="Сохранить в файл", command=save_text_to_file)
    save_button.grid(row=1, column=0, pady=10)

    edit_button = Button(root, text="Редактировать данные", command=lambda: open_edit_file_dialog())
    edit_button.grid(row=1, column=1, pady=10)

    def open_edit_file_dialog():
        file_path = filedialog.askopenfilename(filetypes=[("CSV Files", "*.csv")])
        if file_path:
            edit_csv_file(file_path)

    root.mainloop()

def main():
    for key in ['products_path', 'categories_path', 'product_category_path']:
        config['data'][key] = os.path.join(base_path, config['data'][key])
        if not os.path.isfile(config['data'][key]):
            logger.error(f"File not found: {config['data'][key]}")
            raise FileNotFoundError(f"File not found: {config['data'][key]}")
        else:
            logger.info(f"Found file: {config['data'][key]}")

    logger.info("Начало извлечения данных")
    products_df, categories_df, product_category_df = extract_data(spark, config)

    logger.info("Начало трансформации данных")
    product_category_pairs_df, products_without_categories_df = transform_data(products_df, categories_df, product_category_df)

    logger.info("Процесс завершен. Отображение результатов.")
    display_results(product_category_pairs_df, products_without_categories_df)

if __name__ == "__main__":
    main()