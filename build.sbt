ThisBuild / version := "0.0.1-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.8"

lazy val http4sVersion    = "1.0.0-M36"
lazy val circeVersion     = "0.14.2"
lazy val scalatestVersion = "3.2.13"
lazy val luceneVersion    = "9.4.0"

lazy val root = (project in file("."))
  .settings(
    name := "hnscrape",
    libraryDependencies ++= Seq(
      "org.typelevel"          %% "cats-effect"                % "3.3.14",
      "org.scalatest"          %% "scalatest"                  % scalatestVersion % "test",
      "ch.qos.logback"          % "logback-classic"            % "1.4.0",
      "io.circe"               %% "circe-core"                 % circeVersion,
      "io.circe"               %% "circe-generic"              % circeVersion,
      "io.circe"               %% "circe-generic-extras"       % circeVersion,
      "io.circe"               %% "circe-parser"               % circeVersion,
      "org.http4s"             %% "http4s-dsl"                 % http4sVersion,
      "org.http4s"             %% "http4s-blaze-server"        % http4sVersion,
      "org.http4s"             %% "http4s-blaze-client"        % http4sVersion,
      "org.http4s"             %% "http4s-circe"               % http4sVersion,
      "com.github.pathikrit"   %% "better-files"               % "3.9.1",
      "com.opencsv"             % "opencsv"                    % "5.7.0",
      "org.scala-lang.modules" %% "scala-parallel-collections" % "1.0.4",
      "org.apache.commons"      % "commons-math3"              % "3.6.1",
      "org.apache.lucene"       % "lucene-core"                % luceneVersion,
      "org.apache.lucene"       % "lucene-analysis-common"     % luceneVersion
    ),
    Compile / mainClass := Some("ai.metarank.hnscrape.Main"),
    ThisBuild / assemblyMergeStrategy := {
      case PathList("module-info.class")         => MergeStrategy.discard
      case x if x.endsWith("/module-info.class") => MergeStrategy.discard
      case x =>
        val oldStrategy = (ThisBuild / assemblyMergeStrategy).value
        oldStrategy(x)
    }
  )
