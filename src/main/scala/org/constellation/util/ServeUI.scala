package org.constellation.util

import java.io.File
import java.net.URI
import java.nio.file.{FileSystems, Paths}
import java.util

import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpResponse}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.typesafe.scalalogging.Logger

import scala.io.Source
import scala.util.Try

trait ServeUI {

  val logger: Logger
  val jsPrefix : String

  def jsRequest: Route = {
    pathPrefix("ui") {
      get {
        extractUnmatchedPath { path =>
          logger.info(s"UI Request $path")
          if (jsPrefix == "./ui/ui") {
                  val src = getClass.getResource("/ui/ui" + path)
            /*            val string = src.toString
                        logger.info(s"String src URL : $string")
                        val array = string.split("!")
                        logger.info(s"Serve ui array ${array.toSeq}")
                        val fs = FileSystems.newFileSystem(URI.create(array(0)), new util.HashMap[String, String]())
                        val pathA = fs.getPath(array(1))*/
               val fs = FileSystems.getDefault
               val fsPath = FileSystems.getDefault.getPath(src.toString)
            //   fsPath.toFile

            /*Try{
              getClass.getResourceAsStream("/ui/ui" + path)
            }
            */

            //     new File(src.getPath)
         //   getFromResource("/ui/ui" + path)
            //   val src = Source.fromResource("/ui/ui" + path)
            //val pathA = Paths.get(src.toURI)
            getFromFile(fsPath.toFile)

          } else {
            val file = new File(jsPrefix + path.toString)
            getFromFile(file)
          }
        }
      }
    }
  }

  def serveMainPage: Route = complete { //complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, "<h1>Say hello to akka-http</h1>"))
    logger.info(s"Serve main page jsPrefix: $jsPrefix")

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
                  |<script src="ui-fastopt.js" type="text/javascript"></script>
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
