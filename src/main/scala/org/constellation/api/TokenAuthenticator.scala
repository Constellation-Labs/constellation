package org.constellation.api

import akka.http.scaladsl.server.directives.Credentials
import com.typesafe.scalalogging.StrictLogging
import org.constellation.ConfigUtil

trait TokenAuthenticator extends StrictLogging {

  def basicTokenAuthenticator(credentials: Credentials): Option[String] =
    credentials match {
      case p @ Credentials.Provided(id) if id == ConfigUtil.getAuthId && p.verify(ConfigUtil.getAuthPassword) =>
        Some(id)
      case _ =>
        logger.debug("Bad credentials")
        None
    }
}
