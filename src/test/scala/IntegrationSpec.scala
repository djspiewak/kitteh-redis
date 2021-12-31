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

import cats.effect.{IO, Resource}
import cats.effect.testing.specs2.CatsResource
import cats.syntax.all._

import com.comcast.ip4s._

import dev.profunktor.redis4cats.Redis
import dev.profunktor.redis4cats.connection.RedisClient
import dev.profunktor.redis4cats.data.{RedisChannel, RedisCodec}
import dev.profunktor.redis4cats.effect.Log.Stdout._
import dev.profunktor.redis4cats.pubsub.PubSub

import fs2.Stream

import org.specs2.mutable.SpecificationLike

import scala.concurrent.duration._

class IntegrationSpec
    extends CatsResource[IO, Server[IO]]
    with SpecificationLike {

  val resource = Server[IO](host"0.0.0.0")
  val connect = Redis[IO].utf8("redis://0.0.0.0")

  "real client integration" should {
    "support basic set/get" in withResource { _ =>
      connect use { client =>
        for {
          before <- client.get("test1")
          _ <- client.set("test1", "test value")
          after <- client.get("test1")

          _ <- IO {
            before must beNone
            after must beSome("test value")
          }
        } yield ok
      }
    }

    "set on one client, get on another" in withResource { _ =>
      (connect, connect).tupled use {
        case (client1, client2) =>
          for {
            before1 <- client1.get("test2")
            before2 <- client2.get("test2")

            _ <- IO {
              before1 must beNone
              before2 must beNone
            }

            _ <- client1.set("test2", "can you read me?")
            after <- client2.get("test2")

            _ <- IO(after must beSome("can you read me?"))
          } yield ok
      }
    }

    "publish from one client, subscribe on the other" in withResource { _ =>
      val makeClient = RedisClient[IO].from("redis://localhost")

      val result = (makeClient, makeClient).tupled flatMap {
        case (client1, client2) =>
          val channel = RedisChannel("topic1")

          for {
            pubsub1 <- PubSub.mkPubSubConnection[IO, String, String](client1, RedisCodec.Utf8)
            pubsub2 <- PubSub.mkPubSubConnection[IO, String, String](client2, RedisCodec.Utf8)

            results = pubsub1.subscribe(channel).take(3).compile.toList
            send = Stream("can", "you", "hear", "me", "louder").through(pubsub2.publish(channel)).compile.drain

            (firstThree, _) <- Resource.eval((results, IO.sleep(1.second) *> send).parTupled)
            _ <- Resource.eval(IO(firstThree mustEqual List("can", "you", "hear")))
          } yield ok
      }

      result.use(IO.pure(_))
    }
  }
}
