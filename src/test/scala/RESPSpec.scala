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

import org.scalacheck.{Arbitrary, Gen}

import org.specs2.ScalaCheck
import org.specs2.matcher.Matcher
import org.specs2.mutable.Specification

import scodec.{Attempt, DecodeResult}
import scodec.bits._

class RESPSpec extends Specification with ScalaCheck {

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
        RESP.Array.Full(List(RESP.Int(1), RESP.Int(2), RESP.Int(3))) must encodeAs("*3\r\n:1\r\n:2\r\n:3\r\n")
      }

      "nested array" >> {
        RESP.Array.Full(List(RESP.Array.Full(List(RESP.String.Simple("hi"), RESP.String.Simple("there"))))) must encodeAs(
          "*1\r\n*2\r\n+hi\r\n+there\r\n")
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
        "*2\r\n:1234\r\n+hi there!\r\n" must decodeAs(RESP.Array.Full(List(RESP.Int(1234), RESP.String.Simple("hi there!"))))
      }

      "nested array" >> {
        "*1\r\n*1\r\n+recursion\r\n" must decodeAs(
          RESP.Array.Full(List(RESP.Array.Full(List(RESP.String.Simple("recursion"))))))
      }

      "nil" >> {
        "*-1\r\n" must decodeAs(RESP.Array.Nil)
      }

      "simple HELLO" >> {
        val input = "*2\r\n$5\r\nHELLO\r\n$1\r\n3\r\n"
        input must decodeArrayAs(RESP.Array.Full(List(RESP.String.Bulk.Ascii("HELLO"), RESP.String.Bulk.Ascii("3"))))
      }

      "simple hello (encoded)" >> {
        val input = hex"0x2a320d0a24350d0a48454c4c4f0d0a24310d0a330d0a"
        println(new String(input.toArray))
        RESP.array.decode(input.bits).toEither must beRight
      }
    }
  }

  "round-trip" >> prop { (expected: RESP) =>
    RESP.codec.encode(expected) must beLike {
      case Attempt.Successful(bits) =>
        RESP.codec.decode(bits) must beLike {
          case Attempt.Successful(DecodeResult(result, remainder)) =>
            remainder.isEmpty must beTrue
            result mustEqual expected
        }
    }
  }

  private implicit def arbitraryRESP: Arbitrary[RESP] = {
    import Arbitrary.arbitrary

    def genResp(depth: Int): Gen[RESP] =
      Gen.oneOf(genInt, genString, genArray(depth))

    def genInt: Gen[RESP.Int] =
      arbitrary[Int].filter(_ != Int.MinValue).map(i => RESP.Int(i.abs))

    def genString: Gen[RESP.String] =
      Gen.oneOf(genStringSimple, genStringError, genStringBulk)

    def genStringSimple: Gen[RESP.String.Simple] =
      arbitrary[String].map(RESP.String.Simple(_))

    def genStringError: Gen[RESP.String.Error] =
      arbitrary[String].map(RESP.String.Error(_))

    def genStringBulk: Gen[RESP.String.Bulk] =
      Gen.oneOf(genStringBulkFull, genStringBulkNil)

    def genStringBulkFull: Gen[RESP.String.Bulk.Full] =
      arbitrary[Array[Byte]].map(ByteVector(_)).map(RESP.String.Bulk.Full(_))

    def genStringBulkNil: Gen[RESP.String.Bulk.Nil.type] =
      Gen.const(RESP.String.Bulk.Nil)

    def genArray(depth: Int): Gen[RESP.Array] =
      Gen.oneOf(genArrayFull(depth), genArrayNil)

    def genArrayFull(depth: Int): Gen[RESP.Array.Full] =
      if (depth <= 0)
        Gen.const(RESP.Array.Full(Nil))
      else
        Gen.listOf(genResp(depth - 1)).map(RESP.Array.Full(_))

    def genArrayNil: Gen[RESP.Array.Nil.type] =
      Gen.const(RESP.Array.Nil)

    Arbitrary(genResp(4))
  }

  private def decodeAs(resp: RESP): Matcher[String] = { (str: String) =>
    val result = RESP.codec.decode(BitVector(str.getBytes))
    val test = result == Attempt.successful(DecodeResult(resp, BitVector.empty))
    (test, s"$str decoded to $resp", s"$str decoded to $result; expected $resp")
  }

  private def decodeArrayAs(resp: RESP.Array): Matcher[String] = { (str: String) =>
    val result = RESP.array.decode(BitVector(str.getBytes))
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
