package org.constellation.util

import akka.http.scaladsl.model.{ContentTypes, HttpEntity}
import akka.http.scaladsl.server.Directives.{complete, extractUnmatchedPath, get, getFromResource, pathPrefix, _}
import akka.http.scaladsl.server.Route
import com.typesafe.scalalogging.Logger
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
  val superRed = "#FF0000"
  val xmlOrange = "#E8BF6A"
  val mediumBlue = "#6897BB"
  val annotationYellow = "#BBB529"

}

import scalacss.ScalatagsCss._

package object example {
  val CssSettings: Exports with Settings = scalacss.devOrProdDefaults
}

import example.CssSettings._

object MyStyles extends StyleSheet.Inline {
  import dsl._

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
}


trait ServeUI {

  protected val logger: Logger


  private val scalaJsSource = "ui-opt.js"

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
          width := "20%",
          a(
            href := "/",
            img(
              MyStyles.topElement,
              src := "img/constellation-logo-white.svg",
              height := "100px",
              width := "auto",
              paddingLeft := "15px"
            )
          )
        ),
        div(id := "nav", width := "80%")
      )
    )
  }


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


  private val menuView = div(
    id := "left",
    float.left,
    width := "150px",
    height := "100%",
    "asdf"
  )

  val defaultIndexPage : String = {
    html(
      scalatags.Text.all.head(
        scalatags.Text.tags2.title(pageTitle),
        link(rel := "icon", href := "img/favicon.ico"),
        meta(charset := "UTF-8")
      ),
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
          menuView,
          div(
            id := "right",
            paddingLeft := "25px",
            width := "70%",
            display.`inline-block`,
            div(id := "primary")
          )
        ),
        script(
          src := scalaJsSource,
          `type` := "text/javascript"
        ),
        script(jsApplicationMain,
          `type` := "text/javascript")
      )
    ).render
  }


  def serveMainPage: Route = get {
    path("") {
      logger.debug(s"Serve main page")

      val bodyText = ""

      val html = defaultIndexPage

      val entity = HttpEntity(ContentTypes.`text/html(UTF-8)`, html)
      complete(entity)
    }
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
