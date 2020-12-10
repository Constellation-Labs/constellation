terraform {
  backend "s3" {
    bucket = "constellationlabs-tf"
    key = "cluster"
    region = "us-west-1"
  }
}