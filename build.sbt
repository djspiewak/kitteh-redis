/*
 * Copyright 2021 Daniel Spiewak
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

ThisBuild / baseVersion := "0.1"

ThisBuild / organization := "com.codecommit"
ThisBuild / publishGithubUser := "djspiewak"
ThisBuild / publishFullName := "Daniel Spiewak"

ThisBuild / crossScalaVersions := Seq("2.13.7")

val Fs2Version = "3.3.0"
val Log4CatsVersion = "2.5.0"
val Redis4CatsVersion = "1.0.0"

lazy val server = crossProject(JVMPlatform, NativePlatform)
  .settings(
    libraryDependencies ++= Seq(
      "org.typelevel" %%% "cats-effect" % "3.3.14",
      "org.typelevel" %%% "log4cats-core" % Log4CatsVersion,
      "org.scodec" %%% "scodec-core" % "1.11.10",
      "co.fs2" %%% "fs2-io" % Fs2Version,
      "co.fs2" %%% "fs2-scodec" % Fs2Version,
      "io.chrisdavenport" %%% "crossplatformioapp" % "0.1.0",
      "org.typelevel" %% "cats-effect-testing-specs2" % "1.4.0" % Test,
      "org.typelevel" %% "log4cats-noop" % Log4CatsVersion % Test,
      "org.specs2" %% "specs2-scalacheck" % "4.13.1" % Test,
      "dev.profunktor" %% "redis4cats-effects" % Redis4CatsVersion % Test,
      "dev.profunktor" %% "redis4cats-streams" % Redis4CatsVersion % Test
    )
  )
  .jvmSettings(
    libraryDependencies ++= Seq(
      "org.typelevel" %% "log4cats-slf4j" % Log4CatsVersion,
      "org.slf4j" % "slf4j-log4j12" % "2.0.3"
    )
  )
  .nativeSettings {
    libraryDependencies ++= Seq(
      "com.armanbilge" %%% "epollcat" % "0.1.0"
    )
  }
  .nativeEnablePlugins(ScalaNativeBrewedConfigPlugin)

lazy val root = project
  .in(file("."))
  .settings(
    name := "kitteh-redis"
  )
  .aggregate(server.jvm, server.native)
