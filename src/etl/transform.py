
def transform_data(products_df, categories_df, product_category_df):
    product_category_pairs_df = product_category_df.join(
        products_df, on="product_id", how="inner"
    ).join(
        categories_df, on="category_id", how="inner"
    ).select(
        products_df.product_name, categories_df.category_name
    )

    products_without_categories_df = products_df.join(
        product_category_df, on="product_id", how="left_anti"
    ).select(
        products_df.product_name
    )

    return product_category_pairs_df, products_without_categories_df
