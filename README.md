# kitteh-redis

This project attempts to implement a functioning and relatively compliant Redis server using Cats Effect, Fs2, and Scodec.
As much as possible, the goal is to achieve a *realistic* implementation that is idiomatic, tested, and performant.
Unlike many other example project in the Cats ecosystem, this demonstrates a full *application* which achieves a non-trivial goal, and thus may be considered a starting point for general idioms and best-practices.

## JVM

### Build

`sbt "serverJVM / compile"`

### Run

`sbt "serverJVM / run 0.0.0.0"`

### Logging

By default, logging is set to `ERROR` in the `server/jvm/src/main/resources/log4j.properties` file.
To see all logs, change `ERROR` to `DEBUG`.

## Native

### Build

`sbt "serverNative / compile"`

### Run

`sbt "serverNative / run"`

### Linking

`sbt "serverNative / nativeLink"`

The output binary executable will be located at `server/native/target/scala-2.13/server-out`.
