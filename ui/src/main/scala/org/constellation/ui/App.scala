package org.constellation.ui

import org.scalajs.dom
import org.scalajs.dom.raw._
import rx._
import rx.async.Timer

import scala.scalajs.js
import scala.scalajs.js.{Date, JSApp}
import scala.scalajs.js.annotation.JSExport
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scalatags.JsDom.all._

import scala.concurrent.duration._
import upickle.default._
import upickle.default.{macroRW, ReadWriter => RW}


case class Metrics(metrics: Map[String, String])

object Metrics {
  implicit val rw: RW[Metrics] = macroRW
}


case class ChannelValidationInfo(channel: String, valid: Boolean)

object ChannelValidationInfo {
  implicit val rw: RW[ChannelValidationInfo] = macroRW
}

case class BlockUIOutput(
                          id: String,
                          height: Long,
                          parents: Seq[String],
                          channels: Seq[ChannelValidationInfo],
                        )

object BlockUIOutput {
  implicit val rw: RW[BlockUIOutput] = macroRW
}


case class ChannelUIOutput(channels: Seq[String])

object ChannelUIOutput {
  implicit val rw: RW[ChannelUIOutput] = macroRW
}

case class ChannelOpen(
name: String,
jsonSchema: Option[String] = None,
acceptInvalid: Boolean = true
)

object ChannelOpen {
  implicit val rw: RW[ChannelOpen] = macroRW
}


case class SingleChannelUIOutput(
                                  channelOpen: ChannelOpen,
                                  totalNumMessages: Long = 0L,
                                  last25MessageHashes: Seq[String] = Seq(),
                                  genesisAddress: String
                                )

object SingleChannelUIOutput {
  implicit val rw: RW[SingleChannelUIOutput] = macroRW
}


// uPickle does not like options otherwise this would be Option[String]
case class ChannelOpenResponse(errorMessage: String = "Success", genesisHash: String = "")

object ChannelOpenResponse {
  implicit val rw: RW[ChannelOpenResponse] = macroRW
}

case class ChannelSendRequestRawJson(channelId: String, messages: String)

object ChannelSendRequestRawJson {
  implicit val rw: RW[ChannelSendRequestRawJson] = macroRW
}

case class ChannelSendResponse(
errorMessage: String = "Success", messageHashes: Seq[String]
)

object ChannelSendResponse {
  implicit val rw: RW[ChannelSendResponse] = macroRW
}

import upickle._



object App extends JSApp {

  import Ctx.Owner.Unsafe._

  val heartBeat = {
    import rx.async._
    import rx.async.Platform._

    import scala.concurrent.duration._
    Timer(3000.millis)
  }

  val fastHeartbeat = {
    import rx.async._
    import rx.async.Platform._

    import scala.concurrent.duration._
    Timer(1000.millis)
  }

  val metrics: Var[Metrics] = Var(Metrics(Map()))

  def renderMetrics(mainView: HTMLDivElement): Obs = {

    val dash = mainView.appendChild(div(id := "dash").render).asInstanceOf[HTMLDivElement]

    val forms = dash.appendChild(div(id := "forms").render).asInstanceOf[HTMLDivElement]

    val formsSeq = Seq(
      form(
        action := "/setKeyPair",
        "Set node KeyPair:  ",
        input(
          `type` := "text",
          name := "keyPair",
          value := ""
        )
      ),
      form(
        action := "/submitTX",
        "Submit transaction - address:  ",
        input(
          `type` := "text",
          name := "address",
          value := ""
        ),
        "amount:  ",
        input(
          `type` := "text",
          name := "amount",
          value := ""
        ),
        button(
          `type` := "submit",
          "Submit"
        )
      )
    )

    formsSeq.foreach { f =>
      forms.appendChild(f.render)
    }

    val metricsDiv = dash.appendChild(div(id := "metrics").render).asInstanceOf[HTMLDivElement]

    metrics.foreach{ zo =>

  //  }

  //  heartBeat.foreach(_ => {
  //    XHR.get[Metrics](
  //      {
    //      zo: Metrics =>
          //  lastMetrics = Some(zo)
            metricsDiv.innerHTML = ""
            val zmt = zo.metrics.toSeq.map {
              case ("address", v) =>
                "address" -> Seq(a(href := s"/address/$v", v))
              case ("peers", v) =>
                val vs = v
                  .split(" --- ")
                  .map { pr =>
                    val fullStr = pr.split("API: ")
                    val address = fullStr.last.split(" ").head
                    div(fullStr.head, a(href := address, address))
                  }
                  .toSeq
                "peers" -> vs
              case ("last10TXHash", v) =>
                "last10TXHash" -> v
                  .split(",")
                  .map { addr =>
                    div(a(href := s"/txHash/$addr", addr))
                  }
                  .toSeq
              case (k, v) => k -> Seq(div(v))
            }
            val mets = zmt.sortBy(_._1).map {
              case (k, v) =>
                tr(
                  td(k),
                  td(v)
                )
            }
            val tbl = table(
              tr(
                th("Metric Name"),
                th("Metric Value")
              ),
              mets
            ).render
            metricsDiv.appendChild(tbl)
          //  println(z)
//        },
   //     "/metrics"
   //   )
   // })
    }

  }

  def renderChannels(mainView: HTMLDivElement): Unit = {

    val dash = mainView.appendChild(div(id := "dash").render).asInstanceOf[HTMLDivElement]

    val createBox = mainView.appendChild(div(id := "create-box").render).asInstanceOf[HTMLDivElement]

    val channelInfo = mainView.appendChild(
      div(id := "channel-info", paddingTop := 100.px).render
    ).asInstanceOf[HTMLDivElement]

    var creationFormArea : Option[HTMLDivElement] = None

    val create = createBox.appendChild(
      button(
        id := "channel-create",
        `class` := "hover-dark",
        fontSize.`x-large`,
        width := 300.px,
        padding := 10.px,
        color := "white",
        "Create New Channel"
      ).render
    ).asInstanceOf[HTMLDivElement]

    val createFormHolder = createBox.appendChild(div().render).asInstanceOf[HTMLDivElement]

    def inputStylized(header: String, inputName: String, inputValue: String, numRows: Int = 1) = {
      div(
        div(
          header,
          display.`inline-block`,
          paddingRight := 25.px,
          verticalAlign := "top",
          width := 100.px,
          height.auto,
          paddingBottom := 10.px
        ),
        textarea(
          `class` := s"textarea-$inputName",
          `type` := "text",
          paddingLeft := 25.px,
          width := 400.px,
          name := inputName,
          backgroundColor := Colors.ansiDarkGrey,
          color := "white",
          display.`inline-block`,
          fontSize.large,
          rows := numRows,
          inputValue
        ),
        paddingBottom := 10.px
      )
    }
    val jsonSchema: String = """{
                               |  "title":"Sensors data",
                               |  "type":"object",
                               |  "properties":{
                               |    "temperature": {
                               |      "type": "integer",
                               |      "minimum": -100,
                               |      "maximum": 100
                               |    },
                               |    "name": {
                               |      "type": "string",
                               |      "pattern": "^[A-Z]{4,10}$"
                               |    }
                               |  },
                               |  "required":["temperature", "name"]
                               |}""".stripMargin

    create.onclick = (me: MouseEvent) => {

      if (creationFormArea.nonEmpty) {
        creationFormArea.get.innerHTML = ""
        creationFormArea = None

      } else {

        creationFormArea = Some(
          createFormHolder.appendChild(
            div(
              id := "create-form-area",
              paddingTop := 15.px,
              fontSize.larger,
              inputStylized("Name: ", "name", "channel_name"),
              inputStylized("Schema: ", "schema", jsonSchema, jsonSchema.split("\n").length + 1)
            ).render
          ).asInstanceOf[HTMLDivElement]
        )

        val submitChannel = creationFormArea.get.appendChild(
          button(
            id := "channel-submit",
            `class` := "hover-dark",
            fontSize.`x-large`,
            width := 300.px,
            padding := 10.px,
            color := "white",
            "Submit Channel"
          ).render
        ).asInstanceOf[HTMLDivElement]

        val submitInfo = creationFormArea.get.appendChild(div(paddingTop := 15.px).render).asInstanceOf[HTMLDivElement]


        submitChannel.onclick = (me: MouseEvent) => {

          val name = creationFormArea.get.getElementsByClassName("textarea-name")(0).asInstanceOf[HTMLTextAreaElement].value
          val schema = creationFormArea.get.getElementsByClassName("textarea-schema")(0).asInstanceOf[HTMLTextAreaElement].value
          submitInfo.innerHTML = ""
          val submitting = submitInfo.appendChild(div("Submitting...").render).asInstanceOf[HTMLDivElement]

          XHR.post(
            ChannelOpen(
              name,
              Some(schema)
            ),
            { channelOpenResponse: ChannelOpenResponse =>
              submitInfo.appendChild(
                div(
                  div(channelOpenResponse.errorMessage, paddingTop := 15.px),
                  a(href := s"/view/${channelOpenResponse.genesisHash}/channel", channelOpenResponse.genesisHash, paddingTop := 15.px)
                ).render
              ).asInstanceOf[HTMLDivElement]
              println("Channel open response: " + channelOpenResponse)
            },
            "/channel/open"
          )

          //   createFormHolder.innerHTML = ""
          //   creationFormArea = None
          println(s"Submitted channel name: $name with schema: $schema")
        }

      }

      println("create channel")
    }


    XHR.get[ChannelUIOutput](
      { channelUIOutput =>
        val channelHashes = channelUIOutput.channels
        println(s"Channel hashes $channelHashes")
        channelInfo.appendChild(
          div(
            id := "channel-list",
            channelHashes.map{ hash =>
              a(href := s"/view/$hash/channel", hash, paddingTop := 15.px, display.block)
            }
          ).render
        )
      },
      "/data/channels"
    )

  }

  def renderChannel(mainView: HTMLDivElement, channelId: String): Unit = {
    println("render channel")
    val dash = mainView.appendChild(div(id := "dash").render).asInstanceOf[HTMLDivElement]

    val createBox = mainView.appendChild(div(id := "create-box").render).asInstanceOf[HTMLDivElement]

    val channelInfo = mainView.appendChild(
      div(id := "channel-info", paddingTop := 100.px).render
    ).asInstanceOf[HTMLDivElement]

    var creationFormArea : Option[HTMLDivElement] = None

    val create = createBox.appendChild(
      button(
        id := "channel-create",
        `class` := "hover-dark",
        fontSize.`x-large`,
        width := 300.px,
        padding := 10.px,
        color := "white",
        "Send Messages"
      ).render
    ).asInstanceOf[HTMLDivElement]

    val createFormHolder = createBox.appendChild(div().render).asInstanceOf[HTMLDivElement]

    def inputStylized(header: String, inputName: String, inputValue: String, numRows: Int = 1) = {
      div(
        div(
          header,
          display.`inline-block`,
          paddingRight := 25.px,
          verticalAlign := "top",
          width := 100.px,
          height.auto,
          paddingBottom := 10.px
        ),
        textarea(
          `class` := s"textarea-$inputName",
          `type` := "text",
          paddingLeft := 25.px,
          width := 400.px,
          name := inputName,
          backgroundColor := Colors.ansiDarkGrey,
          color := "white",
          display.`inline-block`,
          fontSize.large,
          rows := numRows,
          inputValue
        ),
        paddingBottom := 10.px
      )
    }
    val messageExamples: String =
      """[
        |{"temperature": 20, "name": "SFWEATH"},
        |{"temperature": 25, "name": "NYWEATH"},
        |{"temperature": -500, "name": "asdkldzlxkc"}
        |]""".stripMargin

    create.onclick = (me: MouseEvent) => {


      if (creationFormArea.nonEmpty) {
        creationFormArea.get.innerHTML = ""
        creationFormArea = None

      } else {
        creationFormArea = Some(
          createFormHolder.appendChild(
            div(
              id := "create-form-area",
              paddingTop := 15.px,
              fontSize.larger,
              inputStylized("Messages: ", "messages", messageExamples, messageExamples.split("\n").length + 1)
            ).render
          ).asInstanceOf[HTMLDivElement]
        )

        val submitChannel = creationFormArea.get.appendChild(
          button(
            id := "channel-submit",
            `class` := "hover-dark",
            fontSize.`x-large`,
            width := 300.px,
            padding := 10.px,
            color := "white",
            "Submit Messages"
          ).render
        ).asInstanceOf[HTMLDivElement]

        val submitInfo = creationFormArea.get.appendChild(div(paddingTop := 15.px).render).asInstanceOf[HTMLDivElement]

        submitChannel.onclick = (me: MouseEvent) => {

          val messageJson = creationFormArea.get.getElementsByClassName("textarea-messages")(0).asInstanceOf[HTMLTextAreaElement].value
          submitInfo.innerHTML = ""
          val submitting = submitInfo.appendChild(div("Submitting...").render).asInstanceOf[HTMLDivElement]

          XHR.post(
            ChannelSendRequestRawJson(
              channelId,
              messageJson
            ),
            { channelSendResponse: ChannelSendResponse =>
              submitInfo.appendChild(
                div(
                  div(channelSendResponse.errorMessage, paddingTop := 15.px),
                  channelSendResponse.messageHashes.map { mh =>
                    a(
                      href := s"/view/$mh/message",
                      mh,
                      paddingTop := 15.px,
                      display.block
                    )
                  }
                ).render
              ).asInstanceOf[HTMLDivElement]
              println("Channel send response: " + channelSendResponse)
            },
            "/channel/send/json"
          )

          //   createFormHolder.innerHTML = ""
          //   creationFormArea = None
          println(s"Submitted channelId: $channelId with messageJson: $messageJson")
        }

      }

      println("create channel")
    }

    val channelData: Var[Option[SingleChannelUIOutput]] = Var(None)

    mainView.appendChild(
      div(id := "3d-graph", width := 400.px, height := 400.px).render
    )

    var blockData = Seq[BlockUIOutput]()


    import scala.scalajs.js.DynamicImplicits._


    val forceScript = script(src := "//unpkg.com/3d-force-graph", `type` := "text/javascript").render
    forceScript.onload = (_ : Event) => {

      import js.Dynamic.{ global => g, newInstance => jsnew }

      val initNodes = js.Array(
        js.Dynamic.literal("id" -> 0)
      )
      val initLinks = js.Array()

      val initData =  js.Dynamic.literal(
        "nodes" -> initNodes,
        "links" -> initLinks
      )

      val graph = jsnew(g.ForceGraph3D)()(g.document.getElementById("3d-graph"))

      graph.height(500)
      graph.width(800)
      graph.linkOpacity(0.20)
      graph.linkWidth(2)

      val hashToInt = scala.collection.mutable.HashMap[String, Int]()
      var maxId : Int = 0

      fastHeartbeat.foreach{ _ =>
        XHR.get[Seq[BlockUIOutput]](
          {bd =>

            val newBlocks = bd.filterNot(b => blockData.exists{_.id == b.id})

            val removedNodes : Seq[BlockUIOutput] = blockData.slice(0, newBlocks.size)

            blockData = bd

            newBlocks.foreach{ b =>
              if (!hashToInt.contains(b.id)) {
                hashToInt(b.id) = maxId
                maxId += 1
              }
            }

            val newNodes = newBlocks.sortBy{b => hashToInt(b.id)}.map{
              data =>
              val hasMessageFromThisChannel = data.channels.exists{_.channel == channelId}
              val color = if (hasMessageFromThisChannel) "green" else "yellow"
                js.Dynamic.literal("id" -> hashToInt(data.id), "color" -> color, "hash" -> data.id)
            }

            val newLinks = newBlocks.flatMap{ block =>
              block.parents.filter{p => hashToInt.contains(p)}.map{p => (p, block.id)}
            }.sorted
              .map{ case (srcId, targ) =>
              js.Dynamic.literal("source" -> hashToInt(srcId), "target" -> hashToInt(targ), "sourceHash" -> srcId)
            }

            val previousData = graph.graphData()

            val previousNodeData = previousData.nodes.filter(
              {node: js.Dynamic =>
                val toRemove = removedNodes.exists{_.id == node.hash.toString}
                !toRemove
              }
            )

            val previousLinksData = previousData.links.filter(
              {link: js.Dynamic =>
                val toRemove = removedNodes.exists{_.id == link.sourceHash.toString}
                !toRemove
              }
            )

            removedNodes.foreach{ b =>
              hashToInt.remove(b.id)
            }

            val data = js.Dynamic.literal(
              "nodes" -> previousNodeData.concat(js.Array(newNodes:_*)),
              "links" -> previousLinksData.concat(js.Array(newLinks:_*))
            )
            graph.graphData(data)


          },
          s"/data/blocks"
        )
      }


    }

    dom.document.head.appendChild(forceScript)


    mainView.appendChild(
      div(
        paddingTop := 100.px,
        div("Channel Info", display.block),
        div(
          ""
        )
      ).render
    )

/*
    heartBeat.foreach{ _ =>
      XHR.get[Option[SingleChannelUIOutput]](
        {cd => channelData() = cd},
        s"/data/channel/$channelId/info"
      )
    }
*/


  }
  // Scala.js code
  @js.native
  trait ForceGraph extends js.Object {
    def graphData(data: js.Dynamic): Unit = js.native
  }


  def updateMetrics(): Unit = {
    heartBeat.foreach(_ => {
      XHR.get[Metrics](
        {
          metrics_ : Metrics =>
          metrics() = metrics_
        },
        "/metrics"
      )
    })
  }

  @JSExport
  def main(): Unit = {

    println("App UI started")

    val mainView = dom.document.getElementById("primary").asInstanceOf[HTMLDivElement]

    val pathName = dom.document.location.pathname
    println(s"Path name: $pathName")

    updateMetrics()

    val navBarTop = dom.document.getElementById("nav").asInstanceOf[HTMLDivElement]
    import scalatags.JsDom.all._
    import org.scalajs.dom.raw._
    import rx._
    import scalatags.rx.all._
    import Ctx.Owner.Unsafe._


    def navMetric(metricName: String, displayName: String) = {
      div(
        display.`inline-block`,
        padding := 10.px,
        paddingLeft := 40.px,
        paddingRight := 25.px,
        fontSize.`x-large`,
        Rx{
            val metricValue = metrics().metrics.getOrElse(metricName, "0")
            s"$metricValue $displayName"
          }
      )
    }

    navBarTop.appendChild(
      Seq(
        navMetric("checkpointAccepted", "blocks"),
        navMetric("transactionAccepted", "TX")
      ).render
    )


    val splitPath = pathName.split("\\/")
    println(s"Split path ${splitPath.toSeq}")
    splitPath.lastOption match {
      case None => renderMetrics(mainView)
      case Some(path) => path match {
        case "channels" =>
          renderChannels(mainView)
        case "channel" =>
          val channelId = splitPath(2)
          renderChannel(mainView, channelId)
        case "message" =>

        case _ => renderMetrics(mainView)
      }
    }



  }

}
