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

import org.specs2.matcher.Matcher
import org.specs2.mutable.Specification

import scodec.{Attempt, DecodeResult}
import scodec.bits.{BitVector, ByteVector}

class RESPSpec extends Specification {

  "encoding" >> {
    ok
  }

  "decoding" >> {
    "ints" >> {
      "positive" >> {
        ":42\r\n" must decodeAs(RESP.Int(42))
      }

      "long" >> {
        ":1234567890\r\n" must decodeAs(RESP.Int(1234567890))
      }

      "negative" >> {
        ":-1\r\n" must decodeAs(RESP.Int(-1))
      }
    }

    "strings" >> {
      "simple" >> {
        "ascii" >> {
          "+abc123 sup?\r\n" must decodeAs(RESP.String.Simple("abc123 sup?"))
        }

        "utf-8" >> {
          "+🥺\r\n" must decodeAs(RESP.String.Simple("🥺"))
        }
      }

      "error" >> {
        "ascii" >> {
          "-abc123 sup?\r\n" must decodeAs(RESP.String.Error("abc123 sup?"))
        }

        "utf8" >> {
          "-🐿\r\n" must decodeAs(RESP.String.Error("🐿"))
        }
      }

      "bulk" >> {
        "full" >> {
          "$50\r\nthis is a test\r\nof the emergency\r\nbroadcast system\r\n" must decodeAs(
            RESP.String.Bulk.Full(ByteVector("this is a test\r\nof the emergency\r\nbroadcast system".getBytes)))
        }

        "nil" >> {
          "$-1\r\n" must decodeAs(RESP.String.Bulk.Nil)
        }
      }
    }

    "arrays" >> {
      "ints and strings" >> {
        "*2\r\n:1234\r\n+hi there!\r\n\r\n" must decodeAs(RESP.Array.Full(List(RESP.Int(1234), RESP.String.Simple("hi there!"))))
      }

      "nested array" >> {
        "*1\r\n*1\r\n+recursion\r\n\r\n\r\n" must decodeAs(
          RESP.Array.Full(List(RESP.Array.Full(List(RESP.String.Simple("recursion"))))))
      }

      "nil" >> {
        "*-1\r\n" must decodeAs(RESP.Array.Nil)
      }
    }
  }

  private def decodeAs(resp: RESP): Matcher[String] = { (str: String) =>
    val result = RESP.codec.decode(BitVector(str.getBytes))
    val test = result == Attempt.successful(DecodeResult(resp, BitVector.empty))
    (test, s"$str decoded to $resp", s"$str decoded to $result; expected $resp")
  }
}
