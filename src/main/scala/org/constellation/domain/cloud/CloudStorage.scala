package org.constellation.domain.cloud

import better.files.File

trait CloudStorage[F[_]] {
  def upload(files: Seq[File], dir: Option[String] = None): F[List[String]]
}
