# Databricks notebook source
"""Generative AI Demo
This notebook demonstrates how to call a Databricks foundation model using
the `databricks-genai` Python package. It assumes you have access to
foundation models in your workspace and that your environment is
configured with Databricks authentication.
"""

# COMMAND ----------

# Authenticate using the Databricks SDK.  The WorkspaceClient picks up
# credentials from your Databricks configuration or environment
# variables (e.g. DATABRICKS_HOST and DATABRICKS_TOKEN).
from databricks.sdk import WorkspaceClient
from databricks_genai import GenerativeModel

# Initialize the client; this will fail if credentials are not configured.
client = WorkspaceClient()

# COMMAND ----------

# Create the model object. Replace the model name with one that your
# workspace has access to, for example "databricks-dbrx-instruct" or
# another available foundation model.
model = GenerativeModel("databricks-dbrx-instruct")

# Generate text from a prompt.
prompt = "Explain the importance of data governance in two sentences."
response = model.generate(prompt)
print(response)

# COMMAND ----------

# Optionally, you can invoke another notebook that uses foundation
# models via dbutils.notebook.run. Uncomment and update the path to
# call a different notebook from this one.
# dbutils.notebook.run("/Path/To/OtherNotebook", timeout_seconds=0)
