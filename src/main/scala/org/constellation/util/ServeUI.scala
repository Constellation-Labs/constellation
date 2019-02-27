package org.constellation.util

import akka.http.scaladsl.model.{ContentTypes, HttpEntity}
import akka.http.scaladsl.server.Directives.{
  complete,
  extractUnmatchedPath,
  get,
  getFromResource,
  pathPrefix,
  _
}
import akka.http.scaladsl.server.{Route, StandardRoute}
import com.typesafe.scalalogging.Logger
import org.constellation.DAO
import scalacss.defaults.Exports
import scalacss.internal.mutable.Settings
import scalatags.Text.TypedTag

/**
  * IntelliJ Darcula
  */
object Colors {

  val methodGold = "#FFC66D"
  val keywordOrange = "#CC7832"
  val bgGrey = "#2B2B2B"
  val valPurple = "#9876AA"
  val lightBlue = "#A9B7C6"
  val commentGrey = "#808080"
  val ansiGrey = "#555555"
  val ansiDarkGrey = "#1F1F1F"
  val darkGrey = "#424242"
  val darkerBlackBlue = "#00001a"
  val blackBlue = "#000033"
  val commentGreen = "#629755"
  val commentGreenBright = "#77B767"
  val superRed = "#FF0000"
  val xmlOrange = "#E8BF6A"
  val intBlue = "#6897BB"
  val annotationYellow = "#BBB529"
  val brightGreen = "#036B13"
  val whiteTextDefault = "#A9B7C6"

}

import scalacss.ScalatagsCss._

package object example {
  val CssSettings: Exports with Settings = scalacss.devOrProdDefaults
}

import example.CssSettings._

object MyStandalone extends StyleSheet.Standalone {
  import dsl._

  "a" - (
    color := Colors.intBlue
  )

}

object MyStyles extends StyleSheet.Inline {
  import dsl._

  val hoverDark: StyleA = style("hover-dark")(
    backgroundColor := Colors.ansiGrey,
    &.hover(
      backgroundColor(Color(Colors.ansiDarkGrey))
    )
  )

  val topElement: StyleA = style(
    addClassNames("top", "top-element"),
    display.inlineBlock,
    verticalAlign.middle
  )

  val hrLine: StyleA = style(
    display.block,
    marginTop(0.5.em),
    marginBottom(0.5.em),
    marginLeft.auto,
    marginRight.auto,
    border.inset,
    borderWidth(2.px),
    borderColor(Color(Colors.commentGrey))
  )

  val leftNavLink: StyleA = style(
    addClassNames("menu-link", "symbol"),
    display.block,
    textDecorationLine.none,
    color := "white",
    fontSize.larger,
    &.hover(
      backgroundColor(Color(Colors.ansiDarkGrey))
    ),
    paddingLeft(35.px),
    paddingTop(7.px),
    paddingBottom(7.px)
  )

}

trait ServeUI {

  protected val logger: Logger

  implicit val dao: DAO

  // Use for rebuilding UI quicker without a restart
  private val debugMode = Option(System.getenv("DAG_DEBUG")).nonEmpty

  private val scalaJsSource = if (debugMode) "/ui-fastopt.js" else "/ui-opt.js"

  private val jsApplicationMain = "org.constellation.ui.App().main()"

  private val pageTitle = "Constellation"

  import scalatags.Text.all._

  private val topBar = {
    div(
      id := "top-bg",
      width := "100%",
      backgroundColor := Colors.ansiDarkGrey,
      div(
        id := "top",
        paddingTop := "5px",
        paddingLeft := "10px",
        width := "100%",
        div(
          id := "home",
          //   width := "20%",
          display.`inline-block`,
          a(
            href := "/",
            img(
              MyStyles.topElement,
              src := "/img/constellation-logo-white.svg",
              height := "100px",
              width := "auto",
              paddingLeft := "15px"
            )
          )
        ),
        div(id := "nav", display.`inline-block`)
      )
    )
  }

  // TODO: Split these up into separate navBar views when we have enough that it makes more sense.

  def leftNav(activePage: String): TypedTag[String] = {
    def element(name: String): TypedTag[String] = {
      a(
        MyStyles.leftNavLink,
        href := s"/view/${name.toLowerCase}",
        name,
        if (name.toLowerCase == activePage) backgroundColor := Colors.ansiDarkGrey else div()
      )
    }
    div(
      id := "left",
      float.left,
      width := 150.px,
      height := 100.pct,
      Seq("Config", "Wallet", "Channels", "Explorer", "Metrics", "Peers").map { name =>
        element(name)
      }
    )
  }

  def defaultIndexPage(activePage: String, optPrimary: Option[TypedTag[String]] = None): String = {
    html(
      scalatags.Text.all.head(
        scalatags.Text.tags2.title(pageTitle),
        link(rel := "icon", href := "/favicon.ico"),
        meta(charset := "UTF-8")
      ),
      MyStandalone.render[TypedTag[String]],
      MyStyles.render[TypedTag[String]],
      body(
        backgroundColor := Colors.bgGrey,
        color := "white",
        margin := "0",
        padding := "0",
        topBar,
        div(
          id := "main",
          height := "80%",
          width := "100%",
          paddingTop := "25px",
          leftNav(activePage),
          div(
            id := "right",
            paddingLeft := "25px",
            width := "70%",
            display.`inline-block`,
            div(
              id := "primary",
              optPrimary.getOrElse(Seq[TypedTag[String]]())
            )
          )
        ),
        script(
          src := scalaJsSource,
          `type` := "text/javascript"
        ),
        script(jsApplicationMain, `type` := "text/javascript")
      )
    ).render
  }

  def jsRequest: Route = {
    pathPrefix("ui") {
      get {
        extractUnmatchedPath { path =>
          logger.info(s"UI Request $path")
          val resPath = "ui/ui" + path
          logger.debug(s"Loading resource from $resPath")
          if (debugMode) getFromFile("./ui/target/scala-2.12/ui" + path) // For debugging more quickly
          else getFromResource(resPath)
        }
      }
    }
  }

  def servePage(page: String, optPrimary: Option[TypedTag[String]] = None): StandardRoute = {
    logger.info(s"Serving page $page")
    val html = defaultIndexPage(page, optPrimary)
    val entity = HttpEntity(ContentTypes.`text/html(UTF-8)`, html)
    complete(entity)
  }

  val serveMainPage: Route = get {
    path("view" / Segment) { page =>
      servePage(page)
    } ~
      path("view" / Segment / "channel") { _ =>
        servePage("")
      } ~
      path("view" / Segment / "message") { msgHash =>
        import constellation._

        servePage(
          "",
          Some(
            div(
              id := "message-view",
              dao.messageService.get(msgHash).prettyJson
            )
          )
        )
      } ~
      path("") { servePage("") }
  }

  val imageRoute: Route = get {
    pathPrefix("img") {
      path(Segment) { s =>
        println("Image request " + s)
        getFromResource(s"ui/img/$s")
      }
    }
  }

}
