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

name := "kitteh-redis"

ThisBuild / baseVersion := "0.1"

ThisBuild / organization := "com.codecommit"
ThisBuild / publishGithubUser := "djspiewak"
ThisBuild / publishFullName := "Daniel Spiewak"

ThisBuild / crossScalaVersions := Seq("2.13.7")

val Fs2Version = "3.2.3"

libraryDependencies ++= Seq(
  "org.typelevel" %% "cats-effect" % "3.3.1",
  "org.scodec"    %% "scodec-core" % "1.11.9",
  "co.fs2"        %% "fs2-io"      % Fs2Version,
  "co.fs2"        %% "fs2-scodec"  % Fs2Version,

  "org.typelevel" %% "cats-effect-testing-specs2" % "1.4.0" % Test)
