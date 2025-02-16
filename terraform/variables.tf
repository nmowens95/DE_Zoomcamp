variable "credentials" {
  description = "Credentials"
  default     = "../keys/ny-rides-nate.json"
}


variable "project" {
  description = "Project"
  default     = "ny-rides-nate"
}


variable "region" {
  description = "Region"
  default     = "us-central1"
}


variable "location" {
  description = "Project Location"
  default     = "US"
}

variable "bq_dataset_name" {
  description = "My bigquery Dataset Name"
  default     = "ny_taxi"
}

variable "gcs_bucket_name" {
  description = "My storage bucket name"
  default     = "ny-rides-nate"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "Standard"
}