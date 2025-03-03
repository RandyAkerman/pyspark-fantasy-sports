import pytest
from pyspark_fantasy_sports import *
import os

def test_stock(spark):
    spark.dataSource.register(StockDataSource)
    stock_api_key = os.getenv('STOCK_API')
    df = spark.read.format("stock").option("api_key", stock_api_key).load("SPY")
    assert df.count() > 0
    # assert df.columns == ["date", "open", "high", "low", "close", "volume", "symbol"]
    # assert df.schema["date"].dataType == "string"
    # assert df.schema["open"].dataType == "double"
    # assert df.schema["high"].dataType == "double"
    # assert df.schema["low"].dataType == "double"
    # assert df.schema["close"].dataType == "double"
    # assert df.schema["volume"].dataType == "long"
    # assert df.schema["symbol"].dataType == "string"