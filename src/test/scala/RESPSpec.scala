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
    "ints" >> {
      "positive" >> {
        RESP.Int(24) must encodeAs(":24\r\n")
      }

      "long" >> {
        RESP.Int(987654321) must encodeAs(":987654321\r\n")
      }

      "negative" >> {
        RESP.Int(-42) must encodeAs(":-42\r\n")
      }
    }

    "strings" >> {
      "simple" >> {
        "ascii" >> {
          RESP.String.Simple("hello") must encodeAs("+hello\r\n")
        }

        "utf8" >> {
          RESP.String.Simple("🐈‍⬛") must encodeAs("+🐈‍⬛\r\n")
        }
      }

      "error" >> {
        "ascii" >> {
          RESP.String.Error("hello") must encodeAs("-hello\r\n")
        }

        "utf8" >> {
          RESP.String.Error("🐈‍⬛") must encodeAs("-🐈‍⬛\r\n")
        }
      }

      "bulk" >> {
        "full" >> {
          val contents = "there's a lot of bytes which need to go here"
          RESP.String.Bulk.Full(ByteVector(contents.getBytes)) must encodeAs(s"$$${contents.length}\r\n$contents\r\n")
        }

        "nil" >> {
          RESP.String.Bulk.Nil must encodeAs("$-1\r\n")
        }
      }
    }

    "array" >> {
      "several ints" >> {
        RESP.Array.Full(List(RESP.Int(1), RESP.Int(2), RESP.Int(3))) must encodeAs("*3\r\n:1\r\n:2\r\n:3\r\n\r\n")
      }

      "nested array" >> {
        RESP.Array.Full(List(RESP.Array.Full(List(RESP.String.Simple("hi"), RESP.String.Simple("there"))))) must encodeAs(
          "*1\r\n*2\r\n+hi\r\n+there\r\n\r\n\r\n")
      }

      "nil" >> {
        RESP.Array.Nil must encodeAs("*-1\r\n")
      }
    }
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

  private def encodeAs(str: String): Matcher[RESP] = { (resp: RESP) =>
    val result = RESP.codec.encode(resp)
    // println(result)
    val test = result == Attempt.successful(BitVector(str.getBytes))
    (test, s"$resp encoded to $str", s"$resp encoded to $result; expected $str")
  }
}
