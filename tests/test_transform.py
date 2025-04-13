
import unittest
from pyspark.sql import SparkSession
from src.etl.transform import transform_data

class TestTransformData(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .master("local[1]") \
            .appName("TestTransformData") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_transform_data(self):
        # Подготовка данных
        products_df = self.spark.createDataFrame([
            (1, 'Product1'),
            (2, 'Product2'),
            (3, 'Product3')
        ], ['product_id', 'product_name'])

        categories_df = self.spark.createDataFrame([
            (1, 'Category1'),
            (2, 'Category2')
        ], ['category_id', 'category_name'])

        product_category_df = self.spark.createDataFrame([
            (1, 1),
            (2, 2)
        ], ['product_id', 'category_id'])

        # Вызов тестируемой функции
        product_category_pairs_df, products_without_categories_df = transform_data(
            products_df, categories_df, product_category_df
        )

        # Проверка пар продукт-категория
        self.assertEqual(product_category_pairs_df.count(), 2)
        expected_pairs = {('Product1', 'Category1'), ('Product2', 'Category2')}
        actual_pairs = set((row['product_name'], row['category_name']) for row in product_category_pairs_df.collect())
        self.assertEqual(actual_pairs, expected_pairs)

        # Проверка продуктов без категорий
        self.assertEqual(products_without_categories_df.count(), 1)
        expected_products_without_categories = {'Product3'}
        actual_products_without_categories = set(row['product_name'] for row in products_without_categories_df.collect())
        self.assertEqual(actual_products_without_categories, expected_products_without_categories)


