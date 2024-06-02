val scala3Version = "3.4.2"
val pekkoVersion = "1.0.2"
val log4jVersion = "2.12.4"
val scalaTestVersion = "3.2.14"

lazy val service = project
  .in(file("."))
  .settings(
    ss = name := "pekko-ratequeue",
    version := "0.1.0-SNAPSHOT",

    scalaVersion := scala3Version,

    libraryDependencies += "org.scalameta" %% "munit" % "1.0.0" % Test,
    libraryDependencies += "org.scalatest" %% "scalatest" % scalaTestVersion,

    libraryDependencies += "org.apache.pekko" %% "pekko-actor-typed" % pekkoVersion,
    libraryDependencies += "org.apache.pekko" %% "pekko-actor-testkit-typed" % pekkoVersion % Test,

    libraryDependencies += "org.apache.logging.log4j" % "log4j-api" % log4jVersion,
    libraryDependencies += "org.apache.logging.log4j" % "log4j-core" % log4jVersion,
    libraryDependencies += "org.apache.logging.log4j" % "log4j-slf4j-impl" % log4jVersion
  )
