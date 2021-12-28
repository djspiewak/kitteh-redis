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
import cats.effect.testing.specs2.CatsEffect
import cats.syntax.all._

import fs2.Stream
import fs2.concurrent.Topic

import org.specs2.mutable.Specification

import scodec.bits.ByteVector

class EvalSpec extends Specification with CatsEffect {
  import Server._

  "ping" >> {
    "empty" >> {
      evalEmpty(Command.Ping(None)) flatMap { results =>
        IO {
          results must beLike {
            case Right(RESP.String.Simple(str) :: Nil) =>
              str mustEqual "PONG"
          }
        }
      }
    }

    "expecting response" >> {
      evalEmpty(Command.Ping(Some("hello"))) flatMap { results =>
        IO {
          results must beLike {
            case Right(RESP.String.Simple(str) :: Nil) =>
              str mustEqual "hello"
          }
        }
      }
    }
  }

  "get" >> {
    "unknown key" >> {
      evalEmpty(Command.Get("bippy")) flatMap { results =>
        IO {
          results must beLeft(Error.Eval.UnknownKey("bippy"))
        }
      }
    }

    "known key" >> {
      val bytes = ByteVector(1, 2, 3)

      evalWithData(Command.Get("bippy"), "bippy" -> bytes) flatMap { results =>
        IO {
          results must beLike {
            case Right((RESP.String.Bulk.Full(bv) :: Nil, _)) =>
              bv mustEqual bytes
          }
        }
      }
    }
  }

  "set" >> {
    "unknown key" >> {
      val bytes = ByteVector(3, 2, 1)

      evalWithData(Command.Set("bippy", bytes)) flatMap { results =>
        IO {
          results must beLike {
            case Right((RESP.String.Simple("OK") :: Nil, data)) =>
              data mustEqual Map("bippy" -> bytes)
          }
        }
      }
    }

    "known key" >> {
      val bytes = ByteVector(3, 2, 1)

      evalWithData(Command.Set("bippy", bytes), "bippy" -> ByteVector(4, 5, 6)) flatMap { results =>
        IO {
          results must beLike {
            case Right((RESP.String.Simple("OK") :: Nil, data)) =>
              data mustEqual Map("bippy" -> bytes)
          }
        }
      }
    }
  }

  "pubsub" >> {
    "subscribe and receive a simple publication" >> {
      val data = ByteVector(1, 2, 3)

      for {
        bippy <- Resource.eval(Topic[IO, ByteVector])

        // subscribe in parallel and push the first result into `results`
        results <- Resource.eval(IO.deferred[ByteVector])
        _ <- evalWithTopics(Command.Subscribe(List("bippy")), "bippy" -> bippy)
          .take(1)
          .compile
          .lastOrError
          .flatMap { e =>
            IO {
              e match {
                case Right(RESP.String.Bulk.Full(data)) => data
                case _ => sys.error("assertion failed")
              }
            }
          }
          .flatMap(results.complete(_))
          .background

        // wait until the subscription is registered
        _ <- Resource.eval(bippy.subscribers.dropWhile(_ < 1).take(1).compile.drain)

        _ <- Resource.eval(bippy.publish1(data))
        back <- Resource.eval(results.get)

        _ <- Resource eval {
          IO {
            back mustEqual data
          }
        }
      } yield ok
    }

    "publish to a known topic" >> {
      val data = ByteVector(1, 2, 3)

      for {
        bippy <- Resource.eval(Topic[IO, ByteVector])

        results <- Resource.eval(IO.deferred[ByteVector])
        _ <- bippy.subscribe(Int.MaxValue).take(1).compile.lastOrError.flatMap(results.complete(_)).background

        response <- Resource.eval(evalWithTopics(Command.Publish("bippy", data), "bippy" -> bippy).compile.toList)

        _ <- Resource eval {
          IO {
            response must beLike {
              case Right(RESP.Int(subscribers)) :: Nil =>
                subscribers mustEqual 1
            }
          }
        }

        back <- Resource.eval(results.get)

        _ <- Resource eval {
          IO {
            back mustEqual data
          }
        }
      } yield ok
    }

    "subscribe, publish, unsubscribe, publish" >> {
      val data1 = ByteVector(1, 2, 3)
      val data2 = ByteVector(1, 2, 3)

      val name = "fubar"

      IO.ref(World.empty[IO, String, ByteVector]) flatMap { worldR =>
        IO.ref(false) flatMap { latch =>
          val client1 = for {
            stateR <- Resource.eval(IO.ref(State.empty[IO, String]))
            received <- eval(Command.Subscribe(List(name)), worldR, stateR).take(1).compile.resource.lastOrError

            _ <- Resource.eval(latch.set(true))
            _ <- Resource.eval(IO(received must beRight(RESP.String.Bulk.Full(data1))))

            _ <- eval(Command.Unsubscribe(List(name)), worldR, stateR).compile.resource.drain
          } yield ()

          val client2 = for {
            stateR <- IO.ref(State.empty[IO, String])
            _ <- (IO.cede *> eval(Command.Publish(name, data1), worldR, stateR).compile.drain).untilM_(latch.get)
            _ <- eval(Command.Publish(name, data2), worldR, stateR).compile.drain
          } yield ()

          (client1.use_, client2).parTupled.as(ok)
        }
      }
    }
  }

  private def evalEmpty(command: Command): IO[Either[Error.Eval, List[RESP]]] = {
    val results = for {
      world <- Stream.eval(IO.ref[World[IO, String, ByteVector]](World(Map(), Map())))
      state <- Stream.eval(IO.ref[State[IO, String]](State(Map())))
      back <- eval(command, world, state)
    } yield back

    results.compile.toList.map(_.sequence)
  }

  private def evalWithData(
      command: Command,
      keys: (String, ByteVector)*)
      : IO[Either[Error.Eval, (List[RESP], Map[String, ByteVector])]] =
    IO.ref[World[IO, String, ByteVector]](World(Map(keys: _*), Map())) flatMap { world =>
      val results = for {
        state <- Stream.eval(IO.ref[State[IO, String]](State(Map())))
        back <- eval(command, world, state)
      } yield back

      results.compile.toList.map(_.sequence) flatMap { results =>
        world.get map { w =>
          results.tupleRight(w.data)
        }
      }
    }

  private def evalWithTopics(
      command: Command,
      topics: (String, Topic[IO, ByteVector])*)
      : Stream[IO, Either[Error.Eval, RESP]] =
    for {
      world <- Stream.eval(IO.ref[World[IO, String, ByteVector]](World(Map(), Map(topics: _*))))
      state <- Stream.eval(IO.ref[State[IO, String]](State(Map())))
      back <- eval(command, world, state)
    } yield back
}
