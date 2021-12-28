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

import cats.effect.{Concurrent, Deferred, Ref, Resource}
import cats.syntax.all._
import cats.conversions.all._

import com.comcast.ip4s._

import fs2.Stream
import fs2.concurrent.Topic
import fs2.interop.scodec.{StreamDecoder, StreamEncoder}
import fs2.io.net.Network

import scodec.bits.ByteVector

final class Server[F[_]: Concurrent] private[kitteh] (world: Ref[F, Server.World[F, String, ByteVector]]) {
  import Server.{State, World}

  private val MaxOutstanding = 1024

  def eval(
      cmd: Command,
      state: Ref[F, State[F, String]])
      : Stream[F, Either[Error.Eval, RESP]] = {
    import Command._

    cmd match {
      case Ping(Some(msg)) =>
        Stream.emit(Right(RESP.String.Simple(msg)))

      case Ping(None) =>
        Stream.emit(Right(RESP.String.Simple("PONG")))

      case Get(key) =>
        Stream.eval(world.get) flatMap {
          case World(data, _) =>
            val results = data.get(key).map(data => Right(RESP.String.Bulk.Full(data)))
            Stream.emit(results.getOrElse(Left(Error.Eval.UnknownKey(key))))
        }

      case Set(key, value) =>
        val eff = world.update(s => s.copy(data = s.data + (key -> value)))
        Stream.eval(eff).as(Right(RESP.String.Simple("OK")))

      case Subscribe(channels) =>
        Stream.eval(state.get) flatMap { st =>
          val filtered = channels.filter(st.subscriptions.contains(_))

          if (!filtered.isEmpty) {
            Stream.emit(Left(Error.Eval.AlreadySubscribed(filtered)))
          } else {
            val makeTopics = channels.traverse(ch => Topic[F, ByteVector].map(ch -> _)) flatMap { topics =>
              world update { w =>
                topics.foldLeft(w) {
                  case (w, (ch, t)) =>
                    if (w.pubsub.contains(ch))
                      w
                    else
                      w.copy(pubsub = w.pubsub + (ch -> t))
                }
              }
            }

            val makeSignals = channels traverse { ch =>
              Deferred[F, Either[Throwable, Unit]].map(ch -> _)
            }

            val eff = makeTopics *> makeSignals flatMap { signals =>
              val update = state update { st =>
                signals.foldLeft(st) {
                  case (st, (ch, d)) =>
                    if (st.subscriptions.contains(ch))
                      st    // we should have already caught this case, but just pessimistically guard
                    else
                      st.copy(subscriptions = st.subscriptions + (ch -> d.complete(Right(())).void))
                }
              }

              update *> world.get map { w =>
                val subscriptions = Stream emits {
                  signals map {
                    case (ch, d) =>
                      val topic = w.pubsub(ch)

                      topic.subscribe(MaxOutstanding)
                        .interruptWhen(d)
                        .map(RESP.String.Bulk.Full(_))
                        .map(Right(_))
                  }
                }

                subscriptions.parJoinUnbounded
              }
            }

            Stream.eval(eff).flatten
          }
        }

      case Unsubscribe(channels) =>
        val toRemove = if (channels.isEmpty) {
          state modify { st =>
            (st.copy(subscriptions = Map()), st.subscriptions)
          }
        } else {
          state modify { st =>
            val (subset, subs2) = channels.foldLeft((Map[String, F[Unit]](), st.subscriptions)) {
              case ((acc, subs), ch) =>
                subs.get(ch) match {
                  case Some(act) => (acc + (ch -> act), subs - ch)
                  case None => (acc, subs)
                }
            }

            (st.copy(subscriptions = subs2), subset)
          }
        }

        val unsubbed = Stream evals {
          toRemove flatMap { subs =>
            subs.values.toSeq.sequence_.as(subs.keys.toList)
          }
        }

        unsubbed.map(ch => Right(RESP.String.Simple(ch)))

      case Publish(channel, message) =>
        val back = Stream eval {
          world.get map { w =>
            w.pubsub.get(channel) match {
              case Some(t) =>
                // TODO cheating!
                t.subscribers.take(1) flatMap { num =>
                  Stream.eval(t.publish1(message)).as(Right(RESP.Int(num)))
                }

              case None =>
                Stream.emit(Left(Error.Eval.UnknownChannel(channel)))
            }
          }
        }

        back.flatten
    }
  }
}

object Server {

  private val MaxConcurrents = 1024
  private val MaxPipelines = 1024

  def apply[F[_]: Concurrent: Network]: Resource[F, Server[F]] = {
    val encoder = StreamEncoder.many(RESP.codec).toPipeByte[F]
    val decoder = StreamDecoder.many(RESP.array).toPipeByte[F]

    // in the beginning we have no data and no pubsub topics
    Resource.eval(Concurrent[F].ref(World.empty[F, String, ByteVector])) flatMap { world =>
      val server = new Server(world)
      val stream = Network[F].server(port = Some(port"6379")) map { client =>
        Stream.eval(Concurrent[F].ref(State.empty[F, String])) flatMap { state =>
          val resp = client.reads.through(decoder)
          val commands = resp.map(Command.parse(_))

          val pipelines = commands.map(_.traverse(server.eval(_, state)).map(_.flatten[Error, RESP]))

          val results = pipelines.parJoin(MaxPipelines)

          val submerged = results map {
            case Left(err) => RESP.String.Error(err.toString)   // TODO
            case Right(resp) => resp
          }

          // TODO logging
          submerged.through(encoder).through(client.writes).drain.handleErrorWith(_ => Stream.empty)
        }
      }

      Stream.emit(server).concurrently(stream.parJoin(MaxConcurrents)).compile.resource.lastOrError
    }
  }

  final case class World[F[_], A, B](data: Map[A, B], pubsub: Map[A, Topic[F, B]])

  object World {
    def empty[F[_], A, B]: World[F, A, B] = World(Map(), Map())
  }

  final case class State[F[_], A](subscriptions: Map[A, F[Unit]])

  object State {
    def empty[F[_], A]: State[F, A] = State(Map())
  }
}
