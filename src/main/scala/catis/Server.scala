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

package catis

import cats.effect.{IO, IOApp, Ref}
import cats.syntax.all._
import cats.conversions.all._

import com.comcast.ip4s._

import fs2.Stream
import fs2.concurrent.Topic
import fs2.interop.scodec.{StreamDecoder, StreamEncoder}
import fs2.io.net.Network

import scodec.bits.ByteVector

object Server extends IOApp.Simple {

  val run = {
    val encoder = StreamEncoder.many(RESP.codec).toPipeByte[IO]
    val decoder = StreamDecoder.many(RESP.array).toPipeByte[IO]

    // in the beginning we have no data and no pubsub topics
    val world = IO.ref(State[IO, String, ByteVector](Map(), Map()))

    val stream = Stream.eval(world) flatMap { state =>
      Network[IO].server(port = Some(port"6379")) map { client =>
        val resp = client.reads.through(decoder)
        val commands = resp.map(Command.parse(_))

        // allow pipelines of up to 1024 commands
        val results = commands.map(_.traverse(eval[IO](_, state)).map(_.flatten[Error, RESP])).parJoin(1024)

        val submerged = results map {
          case Left(err) => RESP.String.Error(err.toString)   // TODO
          case Right(resp) => resp
        }

        // TODO logging
        submerged.through(encoder).through(client.writes).drain.handleErrorWith(_ => Stream.empty)
      }
    }

    // allow up to 1024 concurrent connections
    stream.parJoin(1024).compile.resource.drain.useForever
  }

  def eval[F[_]](cmd: Command, state: Ref[F, State[F, String, ByteVector]]): Stream[F, Either[Error.Eval, RESP]] = ???

  final case class State[F[_], A, B](data: Map[A, B], pubsub: Map[A, Topic[F, B]])
}
