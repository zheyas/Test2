def extract_data(spark, config):
    products_df = spark.read.csv(config['data']['products_path'], header=True, inferSchema=True)
    categories_df = spark.read.csv(config['data']['categories_path'], header=True, inferSchema=True)
    product_category_df = spark.read.csv(config['data']['product_category_path'], header=True, inferSchema=True)
    return products_df, categories_df, product_category_df
