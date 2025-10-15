from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def get_product_category_pairs(products, categories, product_category):
    """
    Возвращает:
    - df_pairs: датафрейм с парами (product_name, category_name)
    - df_orphans: датафрейм с именами продуктов без категорий
    """
    # Join products и product_category
    df_joined = products.join(
        product_category,
        on="product_id",
        how="left"
    ).join(
        categories,
        on="category_id",
        how="left"
    )

    # Пары (product_name, category_name) — где категория есть
    df_pairs = df_joined.filter(
        col("category_name").isNotNull()
    ).select("product_name", "category_name")

    # Продукты без категорий
    df_orphans = df_joined.filter(
        col("category_name").isNull()
    ).select("product_name")

    return df_pairs, df_orphans