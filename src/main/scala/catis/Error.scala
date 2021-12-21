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

sealed trait Error extends Product with Serializable

object Error {
  sealed trait Parse extends Error

  object Parse {
    final case class UnrecognizedCommand(cmd: String) extends Parse
    final case class UnparseableSequence(in: List[RESP]) extends Parse

    final case class UnrecognizedChannelToken(tokens: List[RESP]) extends Parse
    case object EmptySubscription extends Parse

    case object NilCommandArray extends Parse
  }

  sealed trait Eval extends Error

  object Eval {
    final case class UnknownKey(key: String) extends Eval
    final case class AlreadySubscribed(channels: List[String]) extends Eval
  }
}
