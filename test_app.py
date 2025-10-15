import pytest
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf


@pytest.fixture(scope="session")
def spark():
    # Настройка Spark с обходом проблемы getSubject
    conf = SparkConf() \
        .setAppName("TestApp") \
        .setMaster("local[2]") \
        .set("spark.sql.adaptive.enabled", "false") \
        .set("spark.sql.adaptive.coalescePartitions.enabled", "false") \
        .set("spark.authenticate", "false") \
        .set("spark.network.crypto.enabled", "false") \
        .set("spark.sql.warehouse.dir", "/tmp") \
        .set("spark.hadoop.fs.permissions.umask-mode", "022") \
        .set("spark.sql.adaptive.coalescePartitions.minPartitionNum", "1")

    spark_session = SparkSession.builder.config(conf=conf).getOrCreate()
    yield spark_session
    spark_session.stop()


def test_get_product_category_pairs(spark):
    # Подготовка данных
    products_data = [
        (1, "Product A"),
        (2, "Product B"),
        (3, "Product C"),
    ]
    products = spark.createDataFrame(products_data, ["product_id", "product_name"])

    categories_data = [
        (1, "Category X"),
        (2, "Category Y"),
    ]
    categories = spark.createDataFrame(categories_data, ["category_id", "category_name"])

    product_category_data = [
        (1, 1),
        (1, 2),
        (2, 1),
    ]
    product_category = spark.createDataFrame(product_category_data, ["product_id", "category_id"])

    # Вызов функции
    from app import get_product_category_pairs  # Убедись, что файл app.py находится рядом
    pairs, orphans = get_product_category_pairs(products, categories, product_category)

    # Проверка
    pairs_data = pairs.collect()
    assert len(pairs_data) == 3  # (A, X), (A, Y), (B, X)

    orphans_data = orphans.collect()
    assert len(orphans_data) == 1
    assert orphans_data[0]["product_name"] == "Product C"

    # Проверка значений
    expected_pairs = [
        ("Product A", "Category X"),
        ("Product A", "Category Y"),
        ("Product B", "Category X"),
    ]
    actual_pairs = [(row["product_name"], row["category_name"]) for row in pairs_data]
    assert sorted(actual_pairs) == sorted(expected_pairs)
