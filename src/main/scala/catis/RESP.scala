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

// note this is rather naive and we can do a lot better with some shenanigans
object RESP {

  private val asciiInt: Codec[Int] = ascii.xmap(_.toInt, _.toString)

  val int: Codec[Int] = constant(BitVector(':')) ~> crlfTerm(asciiInt)

  object string {
    val simple: Codec[String] = constant(BitVector('+')) ~> crlfTerm(utf8)
    val error: Codec[String] = constant(BitVector('-')) ~> crlfTerm(utf8)

    val bulk: Codec[ByteVector] =
      constant(BitVector('$')) ~> variableSizeBytes(crlfTerm(asciiInt), bytes) <~ constant(crlf)

    val nil: Codec[Unit] = constant(BitVector('$', '-', '1', '\r', '\n'))
  }

  def array[A](inner: Codec[A]): Codec[List[A]] =
    constant(BitVector('*')) ~> variableSizeBytes(crlfTerm(asciiInt), list(inner))

  val nilArray: Codec[Unit] = constant(BitVector('*', '-', '1', '\r', '\n'))

  private def crlfTerm[A](inner: Codec[A]): Codec[A] = variableSizeDelimited(constant(crlf), inner)

  private val crlf = BitVector('\r', '\n')
}
