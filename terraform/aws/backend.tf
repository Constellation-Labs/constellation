terraform {
  backend "s3" {
    bucket = "constellationlabs-dag"
    key = "terraform.tfstate"
    region = "us-west-1"
  }
}