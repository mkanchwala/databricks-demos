# Databricks notebook source
# test-addcol.py
import pytest

from pyspark.sql import SparkSession
from .addcol import with_status

@pytest.fixture
def spark() -> SparkSession:
  return SparkSession.builder.getOrCreate()

def test_with_status(spark):
  source_data = [
    ("pete", "pan", "peter.pan@databricks.com"),
    ("jason", "argonaut", "jason.argonaut@databricks.com")
  ]
  source_df = spark.createDataFrame(
    source_data,
    ["first_name", "last_name", "email"]
  )

  actual_df = with_status(source_df)

  expected_data = [
    ("pete", "pan", "peter.pan@databricks.com", "checked"),
    ("jason", "argonaut", "jason.argonaut@databricks.com", "checked")
  ]

  expected_df = spark.createDataFrame(
    expected_data,
    ["first_name", "last_name", "email", "status"]
  )

  assert(expected_df.collect() == actual_df.collect())
