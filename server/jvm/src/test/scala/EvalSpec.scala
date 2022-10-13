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

import cats.effect.{IO, Ref, Resource}
import cats.effect.testing.specs2.CatsEffect
import cats.syntax.all._

import fs2.Stream
import fs2.concurrent.Topic

import org.specs2.mutable.Specification

import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.noop.NoOpLogger

import scodec.bits.ByteVector

class EvalSpec extends Specification with CatsEffect {
  import Server._

  implicit val logger: Logger[IO] = NoOpLogger[IO]

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
          results must beRight(List(RESP.String.Bulk.Nil))
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

        // subscribe in parallel and assert on the results
        get <- evalWithTopics(Command.Subscribe(List("bippy")), "bippy" -> bippy)
          .take(2)
          .compile
          .toList
          .flatMap { es =>
            IO {
              es must beLike {
                case
                  Right(
                    RESP.Array.Full(List(
                      RESP.String.Bulk.Ascii("subscribe"),
                      RESP.String.Bulk.Ascii("bippy"),
                      RESP.Int(1)))) ::
                  Right(
                    RESP.Array.Full(List(
                      RESP.String.Bulk.Ascii("message"),
                      RESP.String.Bulk.Ascii("bippy"),
                      RESP.String.Bulk.Full(data2)))) ::
                  Nil
                  => data2 mustEqual data
              }

              ()
            }
          }
          .background

        // wait until the subscription is registered
        _ <- Resource.eval(bippy.subscribers.dropWhile(_ < 1).take(1).compile.drain)

        _ <- Resource.eval(bippy.publish1(data))
        _ <- Resource.eval(get.flatMap(_.embedNever))
      } yield ok
    }

    "publish to a known topic" >> {
      val data = ByteVector(1, 2, 3)

      for {
        bippy <- Resource.eval(Topic[IO, ByteVector])

        get <- bippy.subscribe(Int.MaxValue)
          .take(1)
          .compile
          .lastOrError
          .background

        _ <- Resource.eval(bippy.subscribers.dropWhile(_ < 1).take(1).compile.lastOrError)
        response <- Resource.eval(evalWithTopics(Command.Publish("bippy", data), "bippy" -> bippy).compile.toList)

        _ <- Resource eval {
          IO {
            response must beLike {
              case Right(RESP.Int(subscribers)) :: Nil =>
                subscribers mustEqual 1
            }
          }
        }

        back <- Resource.eval(get.flatMap(_.embedNever))

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

            received <- eval(Command.Subscribe(List(name)), worldR, stateR)
              .tail
              .take(1)
              .compile
              .resource
              .lastOrError

            _ <- Resource.eval(latch.set(true))
            _ <- Resource eval {
              IO {
                received must beRight {
                  RESP.Array.Full(
                    List(
                      RESP.String.Bulk.Ascii("message"),
                      RESP.String.Bulk.Ascii(name),
                      RESP.String.Bulk.Full(data1)))
                }
              }
            }

            _ <- eval(Command.Unsubscribe(List(name)), worldR, stateR)
              .compile
              .resource
              .drain
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

  private def eval(
      command: Command,
      worldR: Ref[IO, World[IO, String, ByteVector]],
      stateR: Ref[IO, State[IO, String]])
      : Stream[IO, Either[Error.Eval, RESP]] =
    new Server[IO](Server.DefaultPort, worldR).eval(command, stateR)

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
