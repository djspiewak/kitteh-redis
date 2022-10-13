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

import cats.Applicative
import cats.effect.{Async, Concurrent, Deferred, Ref, Resource}
import cats.syntax.all._

import com.comcast.ip4s._

import fs2.Stream
import fs2.concurrent.Topic
import fs2.interop.scodec.{StreamDecoder, StreamEncoder}
import fs2.io.net.Network

import org.typelevel.log4cats.Logger

import scodec.bits.ByteVector

final class Server[F[_]: Concurrent: Logger] private[kitteh] (
    val port: Port,
    world: Ref[F, Server.World[F, String, ByteVector]]) {

  import Server.{State, World}

  private val MaxOutstanding = 1024

  // hard-coded server header forcing a downgrade if necessary to RESP2
  private val HelloResponse =
    RESP.Array.Full(
      List(
        RESP.String.Simple("server"),
        RESP.String.Simple("redis"),
        RESP.String.Simple("version"),
        RESP.String.Simple("255.255.255"),
        RESP.String.Simple("proto"),
        RESP.Int(2),
        RESP.String.Simple("id"),
        RESP.Int(5),
        RESP.String.Simple("mode"),
        RESP.String.Simple("standalone"),
        RESP.String.Simple("role"),
        RESP.String.Simple("master"),
        RESP.String.Simple("modules"),
        RESP.Array.Full(Nil)))

  // the evaluation model is pretty simple: given a command, produce some errors or some RESP values
  // for most commands, we'll only produce a single error/resp. the main exception to this is
  // Subscribe, which produces a stream of messages from the topic for as long as it is subscribed
  def eval(
      cmd: Command,
      state: Ref[F, State[F, String]])
      : Stream[F, Either[Error.Eval, RESP]] = {
    import Command._

    val prelude = Stream.eval(Logger[F].debug(s"evaluating $cmd")).drain

    // here's the command interpreter. we have one case for each command
    val body = cmd match {
      case Ping(Some(msg)) =>
        Stream.emit(Right(RESP.String.Simple(msg)))

      case Ping(None) =>
        Stream.emit(Right(RESP.String.Simple("PONG")))

      case Hello =>
        Stream.emit(Right(HelloResponse))

      // Get/Set are really simple since all we need to do is interrogate World and render the results
      case Get(key) =>
        Stream.eval(world.get) flatMap {
          case World(data, _) =>
            val results = data.get(key).map(RESP.String.Bulk.Full(_))
            Stream.emit(Right(results.getOrElse(RESP.String.Bulk.Nil)))
        }

      case Set(key, value) =>
        val eff = world.update(s => s.copy(data = s.data + (key -> value)))
        Stream.eval(eff).as(Right(RESP.String.Simple("OK")))

      // this looks scary but it's really all just bookkeeping on World and State
      case Subscribe(channels) =>
        Stream.eval(state.get) flatMap { st =>
          // throw away any subscriptions we already have
          val filtered = channels.filter(st.subscriptions.contains(_))

          if (!filtered.isEmpty) {
            Stream.emit(Left(Error.Eval.AlreadySubscribed(filtered)))
          } else {
            // ensure that fs2 Topics are created in the World. these will be initially subscriber-less
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

            // same idea as above, but instead of creating topics, we're creating the latches which
            // will shut down the subscriptions if Unsubscribe is sent later on in this connection
            val makeSignals = channels traverse { ch =>
              Deferred[F, Either[Throwable, Unit]].map(ch -> _)
            }

            // do the two above things, and then update the local connection state
            val eff = makeTopics *> makeSignals flatMap { signals =>
              val update = state update { st =>
                signals.foldLeft(st) {
                  case (st, (ch, d)) =>
                    // update the connection State with the unsubscription switches
                    if (st.subscriptions.contains(ch))
                      st    // we should have already caught this case, but just pessimistically guard
                    else
                      st.copy(subscriptions = st.subscriptions + (ch -> d.complete(Right(())).void))
                }
              }

              // now that the local state is updated, set up the subscriptions to the World topics
              update *> world.get map { w =>
                val subscriptions = Stream evals {
                  // we need to produce the number of active subscriptions in the responses
                  // we cheat here and list the full number, rather than counting up with each sub
                  state.get.map(_.subscriptions.size) map { num =>
                    signals map {
                      case (ch, d) =>
                        val topic = w.pubsub(ch)

                        val confirmation = RESP.Array.Full(
                          List(
                            RESP.String.Bulk.Ascii("subscribe"),
                            RESP.String.Bulk.Ascii(ch),
                            RESP.Int(num)))

                        def message(bytes: ByteVector): RESP =
                          RESP.Array.Full(
                            List(
                              RESP.String.Bulk.Ascii("message"),
                              RESP.String.Bulk.Ascii(ch),
                              RESP.String.Bulk.Full(bytes)))

                        // MaxOutstanding represents how many unsent messages we allow before backpressure
                        // backpressure in this case will be applied to *publishers*, protecting server memory
                        Stream.emit(Right(confirmation)) ++ topic.subscribe(MaxOutstanding)
                          .interruptWhen(d)   // wire up the kill switch to ensure Unsubscribe works
                          .map(message)    // wrap all responses in RESP packet
                          .map(Right(_))
                    }
                  }
                }

                subscriptions.parJoinUnbounded    // perform all the subscription work in parallel across the channel list
              }
            }

            Stream.eval(eff).flatten
          }
        }

      case Unsubscribe(channels) =>
        // modify our connection State first and produce the list of kill switches set up in Subscribe
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

        // fire all of the kill switches for the unsubscribed channels, gracefully terminating their streams
        // this will automatically remove the Topic subscription and remove the backpressure guards
        val unsubbed = Stream evals {
          toRemove flatMap { subs =>
            subs.values.toSeq.sequence_.as(subs.keys.toList)
          }
        }

        val back = Stream eval {
          state.get.map(_.subscriptions.size) map { before =>
            unsubbed.zipWithIndex map {
              case (ch, i) =>
                Right(
                  RESP.Array.Full(
                    List(
                      RESP.String.Bulk.Ascii("unsubscribe"),
                      RESP.String.Bulk.Ascii(ch),
                      RESP.Int(before - i.toInt))))
            }
          }
        }

        back.flatten

      case Publish(channel, message) =>
        // lots of bookkeeping here just to get at the world state and fire a message to the Topic
        val back = Stream eval {
          world.get map { w =>
            w.pubsub.get(channel) match {
              case Some(t) =>
                // this is the only way fs2 gives us to find the subscriber count
                // it poses a bit of a race condition, but not one that is likely observable
                t.subscribers.take(1) flatMap { num =>
                  // now that we have the Topic, publish the message to all subscribers (if any)
                  // this will asynchronously block if all subscribers are already at MaxOutstanding,
                  // which in turn should mean that the remote client will presumably wait before
                  // sending their next message, since they haven't received an ACK on this one
                  Stream.eval(t.publish1(message)).as(Right(RESP.Int(num)))
                }

              case None =>
                Stream.emit(Left(Error.Eval.UnknownChannel(channel)))
            }
          }
        }

        back.flatten
    }

    // marry the logging to the handler
    prelude ++ body
  }
}

object Server {

  val DefaultPort = port"6379"

  private val DefaultMaxConcurrents = 10000      // match Redis defaults
  private val DefaultMaxPipelines = 1024

  def apply[F[_]: Async: Network](
      host: Host,
      port: Option[Port] = Some(DefaultPort),
      maxPipelines: Int = DefaultMaxPipelines,
      maxConcurrents: Int = DefaultMaxConcurrents)
      : Resource[F, Server[F]] = {

    // encoder/decoder pair for RESP (provided by fs2's scodec interop)
    val encoder = StreamEncoder.many(RESP.codec).toPipeByte[F]
    val decoder = StreamDecoder.many(RESP.array).toPipeByte[F]

    // in the beginning we have no data and no pubsub topics
    val makeWorld = Resource.eval(Concurrent[F].ref(World.empty[F, String, ByteVector]))
    val makeLogger = KittehLogger.resource[F]

    (makeWorld, makeLogger).tupled flatMap {
      case (world, logger0) =>
        implicit val logger: Logger[F] = logger0

        // bind to the specified socket address and restore control flow before handling connections
        Network[F].serverResource(address = Some(host), port = port) flatMap {
          case (addr, requests) =>
            // at this point, the socket is bound, but we haven't received our first request
            val server = new Server(addr.port, world)

            // handle each request individually
            val stream: Stream[F, Stream[F, Nothing]] = requests map { client =>
              val logging = client.remoteAddress.flatMap(isa =>
                Logger[F].debug(s"accepting connection from $isa")
              )
              val init = Concurrent[F].ref(State.empty[F, String])

              Stream.eval(logging *> init) flatMap { state =>
                // everything in here is given an explicit type so as to be easier to follow
                // it actually all type-infers, and originally was written without ascription

                // parse all incoming data as RESP arrays (erroring and closing the connection on failure)
                val resp: Stream[F, RESP.Array] = client.reads.through(decoder)
                // parse RESP values as Redis commands, producing semantic errors on parse failure
                val commands: Stream[F, Either[Error.Parse, Command]] = resp.map(Command.parse(_))

                // we're being non-compliant with pipelines here since we handle them in parallel
                // the way this works is we're parsing commands as fast as we can, *concurrent*
                // with our evaluation of those commands down here

                // the `traverse` is just to get into the Either, and we flatten the evaluation and
                // parsing errors together into Error
                val pipelines: Stream[F, Stream[F, Either[Error, RESP]]] =
                  commands.map(_.traverse(server.eval(_, state)).map(_.flatten[Error, RESP]))

                // here's the parallelism which allows us to evaluate up to maxPipelines commands at once
                // note that this can also reorder output so we're just REALLY being non-compliant all around
                // the only reason we're doing this is because it makes pub/sub easier to encode
                val results: Stream[F, Either[Error, RESP]] = pipelines.parJoin(maxPipelines)

                // render all semantic errors as RESP.Error tokens
                val submerged: Stream[F, RESP] = results evalMap {
                  case Left(err) =>
                    Logger[F].error(s"reporting error: $err").as(RESP.String.Error(err.toString): RESP) // TODO

                  case Right(resp) =>
                    Applicative[F].pure(resp)
                }

                // we need to reply in terms of bytes again, so all the RESP gets encoded on the way out
                val encoded: Stream[F, Byte] = submerged.through(encoder)

                // something a little subtle here is the resulting stream will be continuous so long
                // as the `client.reads` stream continues to produce data, meaning we will sit here
                // and process commands for as long as the client is connected. this is exactly what we want!
                encoded.through(client.writes).drain handleErrorWith { err =>
                  // if we hit any errors, we want to shut down the client socket, but not the server socket
                  // so we just log and call it a day
                  Stream.eval(Logger[F].error(err)("socket handling error")).drain
                }
              }
            }

            // produce the Server[F] while simultaneously handling up to `maxConcurrents` requests simultaneously
            // using .compile.resource means that the background request handling will continue after the
            // Server[F] value is yielded to the caller, but will shut down gracefully when the calling scope closes
            Stream.emit(server).concurrently(stream.parJoin(maxConcurrents)).compile.resource.lastOrError
        }
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
