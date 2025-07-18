# databricks-demos
Databricks Demo Codes

## Generative AI Demo

This repository includes a notebook `notebooks/GenerativeAI_Demo.py` that
shows how to call Databricks foundation models from Python.

### Prerequisites

- Access to the Databricks foundation model APIs in your workspace.
- A cluster running Databricks Runtime 13.3 ML or later.
- Authentication configured for the Databricks SDK (for example using
  environment variables `DATABRICKS_HOST` and `DATABRICKS_TOKEN`).
- `databricks-sdk` and `databricks-genai` Python packages available on the
  cluster (they can be installed with `pip install databricks-sdk databricks-genai`).

### Running the notebook

1. Import `notebooks/GenerativeAI_Demo.py` into your workspace.
2. Attach the notebook to a cluster that meets the prerequisites.
3. Run the notebook cells to authenticate and generate text using the
   configured foundation model.
