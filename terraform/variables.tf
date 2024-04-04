# https://developer.hashicorp.com/terraform/language/values/variables
variable "project" {
  description = "DE Zoomcamp capstone sproject"
  default = "apd311"
}

variable "location" {
  description = "Project's location"
  default = "US"
}

variable "region" {
  default = "us-south1"
}

variable "bq_dataset_name" {
  description = "Big Query Dataset Name"
  default = "apd311"
}

variable "gcs_bucket_name" {
    description = "Storage Bucket Name"
    default = "apd311"
}

variable "gcs_storage_class" {
    description = "Bucket storage class"
    default = "STANDARD"
  
}