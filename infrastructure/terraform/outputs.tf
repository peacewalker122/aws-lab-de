output "lake_buckets" {
  value = { for zone, bucket in aws_s3_bucket.lake : zone => bucket.bucket }
}

output "bucket_name" {
  value = aws_s3_bucket.spike.bucket
}
