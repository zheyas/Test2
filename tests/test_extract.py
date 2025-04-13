
import unittest
from unittest.mock import patch
import pandas as pd
from pyspark.sql import SparkSession
from io import StringIO

# Предположим, что наши функции расположены в `src.main`
from src.etl.extract import extract_data

class TestExtractData(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .master("local[1]") \
            .appName("TestExtractData") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    @patch('pandas.read_csv')
    def test_extract_data(self, mock_read_csv):
        # Prepare mock data
        mock_read_csv.side_effect = [
            pd.DataFrame({'product_id': [1, 2, 2], 'product_name': ['Product1', 'Product2', 'Product2']}),
            pd.DataFrame({'category_id': [1, 2, 2], 'category_name': ['Category1', 'Category2', 'Category2']}),
            pd.DataFrame({'product_id': [1, 2], 'category_id': [1, 2]})
        ]

        config = {
            'data': {
                'products_path': 'path/to/products.csv',
                'categories_path': 'path/to/categories.csv',
                'product_category_path': 'path/to/product_category.csv'
            }
        }

        # Call the function
        products_df, categories_df, product_category_df = extract_data(self.spark, config)

        # Check that duplicates were dropped and data was loaded correctly
        self.assertEqual(products_df.count(), 2)
        self.assertEqual(categories_df.count(), 2)
        self.assertEqual(product_category_df.count(), 2)

        # Check schema if needed
        self.assertTrue('product_id' in products_df.columns)
        self.assertTrue('category_id' in categories_df.columns)

# Запустить тесты
if __name__ == '__main__':
    unittest.main()
