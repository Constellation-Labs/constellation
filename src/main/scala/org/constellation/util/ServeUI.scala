package org.constellation.util

import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpResponse}
import akka.http.scaladsl.server.Directives.{complete, extractUnmatchedPath, get, getFromResource, pathPrefix, _}
import akka.http.scaladsl.server.Route
import com.typesafe.scalalogging.Logger


trait ServeUI {

  val logger: Logger

  def jsRequest: Route = {
    pathPrefix("ui") {
      get {
        extractUnmatchedPath { path =>
          logger.info(s"UI Request $path")
            val resPath = "ui/ui" + path
            logger.debug(s"Loading resource from $resPath")
            getFromResource(resPath)
        }
      }
    }
  }

  def serveMainPage: Route = complete { //complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, "<h1>Say hello to akka-http</h1>"))
    logger.debug(s"Serve main page")

    // val numPeers = (peerToPeerActor ? GetPeers).mapTo[Peers].get().peers.size
    val bodyText = "" // s"Balance: $selfIdBalance numPeers: $numPeers "
    // ^ putting these under metrics, reuse if necessary for other data
    // style="background-color:#060613;color:white"

    val html = s"""<!DOCTYPE html>
                  |<html lang="en">
                  |<head>
                  |    <meta charset="UTF-8">
                  |    <title>Constellation</title>
                  |</head>
                  |<body>
                  |$bodyText
                  |<script src="ui-opt.js" type="text/javascript"></script>
                  |<script type="text/javascript">
                  |org.constellation.ui.App().main()
                  |</script>
                  |</body>
                  |</html>""".stripMargin.replaceAll("\n", "")
    // logger.info("Serving: " + html)

    val entity = HttpEntity(ContentTypes.`text/html(UTF-8)`, html)
    HttpResponse(entity = entity)
    //getFromResource("index.html",ContentTypes.`text/html(UTF-8)`)
  }

}
