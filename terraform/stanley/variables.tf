variable "aws_region" {
  description = "AWS region for all resources."
  type        = string
  default     = "ap-southeast-2"
}

variable "stage_name" {
  description = "Deployment stage name."
  type        = string
  default     = "prod"
}

variable "python_runtime" {
  description = "Python runtime e.g python3.12."
  type        = string
  default     = "python3.12"
}

variable "dap_api_url" {
  description = "Instructure DAP API url."
  type        = string
}

variable "dap_client_id" {
  description = "Instructure DAP client id."
  type        = string
}

variable "dap_client_secret" {
  description = "Instructure DAP client secret."
  type        = string
}

variable "dap_connection_string" {
  description = "Database connection string where canvas-data-2 will be synchronized."
  type        = string
}

variable "tables" {
  description = "Comma seperated list of table names."
  type        = string
}

