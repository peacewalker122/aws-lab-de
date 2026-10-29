variable "endpoint" {
  type    = string
  default = "http://127.0.0.1:4566"

  validation {
    condition     = can(regex("^http://(127\\.0\\.0\\.1|localhost):[0-9]+$", var.endpoint))
    error_message = "The M0 endpoint must be a loopback HTTP URL with a port."
  }
}

variable "region" {
  type    = string
  default = "us-east-1"
}

variable "environment" {
  type    = string
  default = "local"

  validation {
    condition     = can(regex("^[a-z0-9-]+$", var.environment))
    error_message = "Environment must contain only lowercase letters, numbers, and hyphens."
  }
}
