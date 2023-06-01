variable "databricks_host" {
  description = "The Azure Databricks workspace URL."
  type        = string
}

# Initialize the Databricks Terraform provider.
terraform {
  required_providers {
    databricks = {
      source = "databricks/databricks"
    }
  }
}

# Use Azure CLI authentication.
provider "databricks" {
  host = var.databricks_host
}

# Retrieve information about the current user.
data "databricks_current_user" "me" {}