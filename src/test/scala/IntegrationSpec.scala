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

import cats.effect.IO
import cats.effect.testing.specs2.CatsResource
import cats.syntax.all._

import com.comcast.ip4s._

import dev.profunktor.redis4cats.{Redis, RedisCommands}
import dev.profunktor.redis4cats.effect.Log.Stdout._

import org.specs2.mutable.SpecificationLike

class IntegrationSpec
   extends CatsResource[IO, (Server[IO], RedisCommands[IO, String, String])]
   with SpecificationLike {

  val resource = (Server[IO](host"0.0.0.0"), Redis[IO].utf8("redis://0.0.0.0")).tupled

  "real client integration" should {
    "support basic set/get" in withResource {
      case (_, redis) =>
        for {
          before <- redis.get("test1")
          _ <- redis.set("test1", "test value")
          after <- redis.get("test1")

          _ <- IO {
            before must beNone
            after must beSome("test value")
          }
        } yield ok
    }
  }
}
