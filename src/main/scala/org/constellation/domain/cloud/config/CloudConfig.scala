package org.constellation.domain.cloud.config

sealed trait CloudProvider

case class CloudConfig(providers: List[CloudProvider])

object CloudConfig {
  def empty: CloudConfig = new CloudConfig(List.empty)
}

case class S3(
  bucketName: String,
  region: String,
  auth: S3ConfigAuth
) extends CloudProvider

sealed trait S3ConfigAuth

case class S3Inherit() extends S3ConfigAuth

case class Credentials(
  accessKey: String,
  secretKey: String
) extends S3ConfigAuth

case class GCP(
  bucketName: String,
  auth: GCPAuth
) extends CloudProvider

sealed trait GCPAuth

case class PermissionFile(pathToPermissionFile: String) extends GCPAuth
case class GCPInherit() extends GCPAuth

case class S3Compat(
  bucketName: String,
  region: String,
  endpoint: String,
  auth: S3CompatCredentials
) extends CloudProvider

case class S3CompatCredentials(
  accessKey: String,
  secretKey: String
)

case class Local(path: String) extends CloudProvider
