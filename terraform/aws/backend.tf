terraform {
  backend "s3" {
    bucket = "constellationlabs-terraform"
    key = "cluster"
    region = "us-west-1"
  }
}