
import pandas as pd

def extract_data(spark, config):
    # Загрузка данных с помощью Pandas для проверки на дубликаты
    products_df = pd.read_csv(config['data']['products_path'])
    categories_df = pd.read_csv(config['data']['categories_path'])
    product_category_df = pd.read_csv(config['data']['product_category_path'])

    # Удаляем дубликаты по 'product_id' и 'category_id' соответственно
    products_df = products_df.drop_duplicates(subset='product_id')
    categories_df = categories_df.drop_duplicates(subset='category_id')

    # Преобразование обратно в Spark DataFrame
    spark_products_df = spark.createDataFrame(products_df)
    spark_categories_df = spark.createDataFrame(categories_df)
    spark_product_category_df = spark.createDataFrame(product_category_df)

    return spark_products_df, spark_categories_df, spark_product_category_df
