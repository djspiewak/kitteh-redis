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

import org.specs2.mutable.Specification

import scodec.bits.ByteVector

class CommandSpec extends Specification {

  "ping (empty)" >> {
    Command.parse(wrap("PING")) must beRight(Command.Ping(None))
  }

  "ping (non-empty)" >> {
    Command.parse(wrap("PING", "stuff")) must beRight(Command.Ping(Some("stuff")))
  }

  "hello" >> {
    Command.parse(wrap("HELLO")) must beRight(Command.Hello: Command)
  }

  "get" >> {
    Command.parse(wrap("GET", "bippy")) must beRight(Command.Get("bippy"))
  }

  "get (empty)" >> {
    Command.parse(wrap("GET")) must beLeft
  }

  "set" >> {
    Command.parse(wrap("SET", "bippy", "boppity")) must beRight(Command.Set("bippy", ByteVector("boppity".getBytes)))
  }

  "subscribe (three)" >> {
    Command.parse(wrap("SUBSCRIBE", "foo", "bar", "baz")) must beRight(Command.Subscribe(List("foo", "bar", "baz")))
  }

  "subscribe (non-ascii)" >> {
    val wrapped = RESP.Array.Full(List(RESP.String.Bulk.Ascii("SUBSCRIBE"), RESP.String.Bulk.Full(ByteVector("🐈‍⬛".getBytes))))

    Command.parse(wrapped) must beLeft[Error](
      Error.Parse.UnrecognizedChannelToken(List(RESP.String.Bulk.Full(ByteVector("🐈‍⬛".getBytes)))))
  }

  "subscribe (empty)" >> {
    Command.parse(wrap("SUBSCRIBE")) must beLeft[Error](Error.Parse.EmptySubscription)
  }

  "unsubscribe (three)" >> {
    Command.parse(wrap("UNSUBSCRIBE", "foo", "bar", "baz")) must beRight(Command.Unsubscribe(List("foo", "bar", "baz")))
  }

  "unsubscribe (empty)" >> {
    Command.parse(wrap("UNSUBSCRIBE")) must beRight(Command.Unsubscribe(Nil))
  }

  "publish" >> {
    Command.parse(wrap("PUBLISH", "bippy", "stuff")) must beRight(Command.Publish("bippy", ByteVector("stuff".getBytes)))
  }

  "publish (missing data)" >> {
    Command.parse(wrap("PUBLISH", "bippy")) must beLeft
  }

  "nil" >> {
    Command.parse(RESP.Array.Nil) must beLeft[Error](Error.Parse.NilCommandArray)
  }

  private def wrap(tokens: String*): RESP.Array =
    RESP.Array.Full(tokens.map(RESP.String.Bulk.Ascii(_)).toList)
}
