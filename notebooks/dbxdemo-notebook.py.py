# Databricks notebook source
# Databricks notebook source
import sys
sys.path.append("/databricks/python3/lib/python3.8/site-packages")

# COMMAND ----------

import unittest
from addcol import *

class TestNotebook(unittest.TestCase):

  def test_with_status(self):
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

    self.assertEqual(expected_df.collect(), actual_df.collect())

unittest.main(argv = [''], verbosity = 2, exit = False)
