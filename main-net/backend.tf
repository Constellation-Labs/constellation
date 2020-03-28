terraform {
  backend "s3" {
    bucket = "constellationlabs-terraform"
    key = "grafana-mainnet"
    region = "us-west-1"
  }
}