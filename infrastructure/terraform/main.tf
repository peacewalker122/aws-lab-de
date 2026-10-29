resource "aws_s3_bucket" "lake" {
  for_each = toset(["raw", "silver", "gold", "artifacts"])

  bucket        = "aster-${var.environment}-${each.key}"
  force_destroy = false
}

resource "aws_s3_bucket" "spike" {
  bucket        = "aster-${var.environment}-m0-spike"
  force_destroy = false
}
