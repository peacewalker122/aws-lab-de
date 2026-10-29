resource "aws_s3_bucket" "spike" {
  bucket        = "aster-${var.environment}-m0-spike"
  force_destroy = false
}
