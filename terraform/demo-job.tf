variable "job_name" {
  description = "A Sample Databricks Job Name."
  type        = string
  default     = "Demo Job"
}

variable "cluster_id" {
  description = "A Default Cluster ID."
  type        = string
  default     = "none"
}

variable "notebook_path" {
  description = "A Notebook Path."
  type        = string
  default     = "none"
}

resource "databricks_job" "this" {
  name = var.job_name
  existing_cluster_id = var.cluster_id
  notebook_task {
    notebook_path = var.notebook_path
  }
  email_notifications {
    on_success = [ data.databricks_current_user.me.user_name ]
    on_failure = [ data.databricks_current_user.me.user_name ]
  }
}

output "job_url" {
  value = databricks_job.this.url
}