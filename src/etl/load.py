def load_data(data, config):
    # Сохраняем пары "Имя продукта – Имя категории"
    data['product_category_pairs'].write.csv(config['output']['product_category_pairs'], mode='overwrite', header=True)

    # Сохраняем продукты без категорий
    data['product_no_category'].write.csv(config['output']['product_no_category'], mode='overwrite', header=True)
