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

import scodec.Codec
import scodec.bits.{BitVector, ByteVector}
import scodec.codecs._

sealed trait RESP extends Product with Serializable

// note this is rather naive and we can do a lot better with some shenanigans
object RESP {

  private val asciiInt: Codec[scala.Int] = ascii.xmap(_.toInt, _.toString)
  private val crlf = BitVector('\r', '\n')
  private val delimInt: Codec[scala.Int] = crlfTerm(asciiInt)

  lazy val codec: Codec[RESP] =
    discriminated[RESP].by(byte)
      .typecase('+', crlfTerm(utf8).as[String.Simple])
      .typecase('-', crlfTerm(utf8).as[String.Error])
      .typecase(':', delimInt.as[Int])
      .typecase('$', bulk)
      .typecase('*', array)

  val bulk: Codec[String.Bulk] =
    discriminated[String.Bulk].by(lookahead(constant('-')))
      .typecase(true, constant('-', '1', '\r', '\n').xmap[String.Bulk.Nil.type](_ => String.Bulk.Nil, _ => ()))
      .typecase(false, (variableSizeBytes(delimInt, bytes) <~ constant(crlf)).as[String.Bulk.Full])

  lazy val array: Codec[Array] =
    discriminated[Array].by(lookahead(constant('-')))
      .typecase(true, constant('-', '1', '\r', '\n').xmap[Array.Nil.type](_ => Array.Nil, _ => ()))
      .typecase(false, listOfN(delimInt, crlfTerm(lazily(codec))).as[Array.Full])

  private def crlfTerm[A](inner: Codec[A]): Codec[A] = variableSizeDelimited(constant(crlf), inner)

  final case class Int(value: scala.Int) extends RESP

  object String {
    final case class Simple(value: java.lang.String) extends RESP
    final case class Error(value: java.lang.String) extends RESP

    sealed trait Bulk extends RESP

    object Bulk {

      object Ascii {
        def apply(str: String): Full = Full(ByteVector.encodeAscii(str).toOption.get)
        def unapply(contents: ByteVector): Option[String] = contents.decodeAscii.toOption
      }

      final case class Full(contents: ByteVector) extends Bulk
      case object Nil extends Bulk
    }
  }

  sealed trait Array extends RESP

  object Array {
    final case class Full(contents: List[RESP]) extends Array
    case object Nil extends Array
  }
}
