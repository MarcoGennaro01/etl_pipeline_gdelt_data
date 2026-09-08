#Bucket


resource "google_storage_bucket" "data_lake"{
  name = var.gcp_bucket_name
  location = "US"
}
