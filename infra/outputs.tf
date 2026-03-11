output "gcs_bucket_name" {
  description = "Raw data landing bucket"
  value       = google_storage_bucket.mta_raw.name
}

output "bigquery_bronze" {
  description = "Bronze dataset ID"
  value       = google_bigquery_dataset.bronze.dataset_id
}

output "bigquery_silver" {
  description = "Silver dataset ID"
  value       = google_bigquery_dataset.silver.dataset_id
}

output "bigquery_gold" {
  description = "Gold dataset ID"
  value       = google_bigquery_dataset.gold.dataset_id
}