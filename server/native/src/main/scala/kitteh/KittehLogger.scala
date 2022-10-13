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

import cats.effect.{Resource, Sync}
import org.typelevel.log4cats.Logger
import cats.effect.std.Console
import cats.implicits.showInterpolator

final class KittehLogger[F[_]](implicit Console: Console[F]) extends Logger[F] {
  override def error(message: => String): F[Unit] =
    Console.errorln(show"X: $message")

  override def warn(message: => String): F[Unit] =
    Console.println(show"?: $message")

  override def info(message: => String): F[Unit] =
    Console.println(show"|: $message")

  override def debug(message: => String): F[Unit] =
    Console.println(show">: $message")

  override def trace(message: => String): F[Unit] =
    Console.println(show"//: $message")

  override def error(t: Throwable)(message: => String): F[Unit] = error(
    show"$message -- ${t.toString}"
  )

  override def warn(t: Throwable)(message: => String): F[Unit] = warn(
    show"$message -- ${t.toString}"
  )

  override def info(t: Throwable)(message: => String): F[Unit] = info(
    show"$message -- ${t.toString}"
  )

  override def debug(t: Throwable)(message: => String): F[Unit] = debug(
    show"$message -- ${t.toString}"
  )

  override def trace(t: Throwable)(message: => String): F[Unit] = trace(
    show"$message -- ${t.toString}"
  )
}

object KittehLogger {
  def resource[F[_]: Sync]: Resource[F, Logger[F]] =
    Resource
      .pure(Console.make[F])
      .map(implicit console => new KittehLogger[F])
}
