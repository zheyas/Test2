def transform_data(products_df, categories_df, product_category_df):
    # Соединяем датафреймы для получения пар "Имя продукта – Имя категории"
    product_category_pairs = product_category_df.join(
        products_df, "product_id", "inner"
    ).join(
        categories_df, "category_id", "left"
    ).select(
        products_df.product_name, categories_df.category_name
    )

    # Находим все продукты без категорий
    product_no_category = products_df.join(
        product_category_df, "product_id", "left_anti"
    ).select(products_df.product_name)

    return {
        "product_category_pairs": product_category_pairs,
        "product_no_category": product_no_category
    }
