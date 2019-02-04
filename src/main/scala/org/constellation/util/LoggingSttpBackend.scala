package org.constellation.util

import com.softwaremill.sttp.{MonadError, Request, Response, SttpBackend}
import com.typesafe.scalalogging.StrictLogging

/** Documentation. */
class LoggingSttpBackend[R[_], S](delegate: SttpBackend[R, S]) extends SttpBackend[R, S]
  with StrictLogging {

  /** Documentation. */
  override def send[T](request: Request[T, S]): R[Response[T]] = {
    responseMonad.map(responseMonad.handleError(delegate.send(request)) {
      case e: Exception =>
        logger.error(s"Exception when sending request: $request", e)
        responseMonad.error(e)
    }) { response =>
      if (response.isSuccess) {
        logger.debug(s"For request: $request got response: $response")
      } else {
        logger.warn(s"For request: $request got response: $response")
      }
      response
    }
  }

  /** Documentation. */
  override def close(): Unit = delegate.close()

  /** Documentation. */
  override def responseMonad: MonadError[R] = delegate.responseMonad
}
