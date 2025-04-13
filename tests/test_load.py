import unittest
import shutil
import os
from pyspark.sql import SparkSession
from pyspark.sql import Row


class TestLoadData(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .master("local[1]") \
            .appName("TestLoadData") \
            .getOrCreate()

        cls.output_dir = 'test_output'
        os.makedirs(cls.output_dir, exist_ok=True)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        shutil.rmtree(cls.output_dir, ignore_errors=True)

    def test_load_data(self):
        from src.etl.load import load_data

        # Создаем тестовые DataFrame
        product_category_pairs = self.spark.createDataFrame([
            Row(product_name='Product1', category_name='Category1'),
            Row(product_name='Product2', category_name='Category2')
        ])

        product_no_category = self.spark.createDataFrame([
            Row(product_name='Product3')
        ])

        data = {
            'product_category_pairs': product_category_pairs,
            'product_no_category': product_no_category
        }

        config = {
            'output': {
                'product_category_pairs': os.path.join(self.output_dir, 'product_category_pairs.csv'),
                'product_no_category': os.path.join(self.output_dir, 'product_no_category.csv')
            }
        }

        # Выполнение тестируемой функции
        load_data(data, config)

        # Проверка, что файлы созданы
        self.assertTrue(os.path.exists(config['output']['product_category_pairs']))
        self.assertTrue(os.path.exists(config['output']['product_no_category']))

        # Далее можно добавить чтение этих файлов и проверку содержимого
        # Для этого можно перезагрузить данные с помощью Spark и проверить их содержимое
        product_category_pairs_df = self.spark.read.csv(config['output']['product_category_pairs'], header=True)
        product_no_category_df = self.spark.read.csv(config['output']['product_no_category'], header=True)

        self.assertEqual(product_category_pairs_df.count(), 2)
        self.assertEqual(product_no_category_df.count(), 1)

        # Пример проверки конкретных значений
        self.assertEqual(product_category_pairs_df.filter(product_category_pairs_df.product_name == 'Product1').count(),
                         1)
        self.assertEqual(product_no_category_df.filter(product_no_category_df.product_name == 'Product3').count(), 1)

