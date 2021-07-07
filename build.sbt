ThisBuild / scalaVersion := "2.13.6"
ThisBuild / organization := "ru.delimobil"

lazy val root = (project in file("."))
  .settings(
    name := "cabbit",
    version := "0.0.6-SNAPSHOT",
    addCompilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1"),
    libraryDependencies ++= Seq(
      "co.fs2" %% "fs2-core" % "2.5.8",
      "com.rabbitmq" % "amqp-client" % "5.12.0",
      "io.circe" %% "circe-core" % "0.14.1",
      "com.dimafeng" %% "testcontainers-scala-rabbitmq" % "0.39.5" % Test,
      "io.circe" %% "circe-parser" % "0.14.1" % Test,
      "org.scalatest" %% "scalatest" % "3.2.9" % Test,
      "org.slf4j" % "slf4j-simple" % "1.7.31" % Test,
    )
  )
