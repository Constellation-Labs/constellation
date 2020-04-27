terraform {
  backend "s3" {
    bucket = "constellationlabs-tf"
    key = "grafana"
    region = "us-west-1"
  }
}