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

package kitteh

import cats.effect.std.Console
import cats.effect.{ExitCode, IO}
import com.comcast.ip4s.Host
import fs2.io.net.Network
import io.chrisdavenport.crossplatformioapp.CrossPlatformIOApp

object Main extends CrossPlatformIOApp {

  val usage = Console[IO].errorln("usage: ./kitteh host")

  Network[IO].serverResource()

  def run(args: List[String]): IO[ExitCode] =
    args.headOption.flatMap(Host.fromString) match {
      case Some(host) => Server[IO](host).useForever
      case None => usage.as(ExitCode.Error)
    }
}
