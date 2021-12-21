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

import scodec.bits.ByteVector

sealed trait Command extends Product with Serializable

object Command {

  def parse(in: List[RESP]): Either[ParseError, Command] = {
    import RESP._

    in match {
      case String.Bulk.Ascii("PING") :: String.Bulk.Ascii(contents) :: Nil =>
        Right(Ping(Some(contents)))

      case String.Bulk.Ascii("PING") :: Nil =>
        Right(Ping(None))

      case String.Bulk.Ascii("GET") :: String.Bulk.Ascii(key) :: Nil =>
        Right(Get(key))

      case String.Bulk.Ascii("SET") :: String.Bulk.Ascii(key) :: String.Bulk.Full(value) :: Nil =>
        Right(Set(key, value))

      case String.Bulk.Ascii("SUBSCRIBE") :: channels0 =>
        val channels = channels0 collect {
          case String.Bulk.Ascii(name) => name
        }

        if (channels.length != channels0.length)
          Left(ParseError.UnrecognizedChannelToken(channels0))
        else if (channels.isEmpty)
          Left(ParseError.EmptySubscription)
        else
          Right(Subscribe(channels))

      case String.Bulk.Ascii("UNSUBSCRIBE") :: channels0 =>
        val channels = channels0 collect {
          case String.Bulk.Ascii(name) => name
        }

        if (channels.length != channels0.length)
          Left(ParseError.UnrecognizedChannelToken(channels0))
        else
          Right(Unsubscribe(channels))

      case String.Bulk.Ascii("PUBLISH") :: String.Bulk.Ascii(channel) :: String.Bulk.Full(message) :: Nil =>
        Right(Publish(channel, message))

      case String.Bulk.Ascii(cmd) :: _ =>
        Left(ParseError.UnrecognizedCommand(cmd))

      case other =>
        Left(ParseError.UnparseableSequence(other))
    }
  }

  final case class Ping(msg: Option[String]) extends Command
  final case class Get(key: String) extends Command
  final case class Set(key: String, value: ByteVector) extends Command

  final case class Subscribe(channels: List[String]) extends Command
  final case class Unsubscribe(channels: List[String]) extends Command
  final case class Publish(channel: String, message: ByteVector) extends Command

  sealed trait ParseError extends Product with Serializable

  object ParseError {
    final case class UnrecognizedCommand(cmd: String) extends ParseError
    final case class UnparseableSequence(in: List[RESP]) extends ParseError

    final case class UnrecognizedChannelToken(tokens: List[RESP]) extends ParseError
    case object EmptySubscription extends ParseError
  }
}
