import com.scalapenos.sbt.prompt._

///////////////////////////////////////////////////////////////////////////////////////////////////
// Settings
///////////////////////////////////////////////////////////////////////////////////////////////////

lazy val commonSettings = Seq(
  organization := "me.milanvdm",
  scalaVersion := "2.12.10",
  resolvers ++= Seq(
        "Typesafe Releases" at "https://repo.typesafe.com/typesafe/maven-releases/",
        "confluent.io" at "https://packages.confluent.io/maven/"
      ),
  scalafmtOnCompile := true,
  incOptions := incOptions.value.withLogRecompileOnMacro(false),
  scalacOptions ++= commonScalacOptions,
  fork in Test := true,
  parallelExecution in Test := false,
  testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oDF"),
  libraryDependencies ++= Seq(
        compilerPlugin(D.kindProjector)
      ),
  scalacOptions in (Compile, doc) := (scalacOptions in (Compile, doc)).value.filter(_ != "-Xfatal-warnings"),
  promptTheme := PromptTheme(
        List(
          text("[SBT] ", fg(136)),
          currentProject(fg(64)).padRight(": ")
        )
      )
)

lazy val commonScalacOptions = Seq(
  "-deprecation",
  "-encoding",
  "UTF-8",
  "-feature",
  "-language:existentials",
  "-language:higherKinds",
  "-language:implicitConversions",
  "-language:experimental.macros",
  "-unchecked",
  "-Xfatal-warnings",
  "-Xfuture",
  "-Xlint",
  "-Xlog-reflective-calls",
  "-Ydelambdafy:method",
  "-Yno-adapted-args",
  "-Ypartial-unification",
  "-Ywarn-dead-code",
  "-Ywarn-value-discard",
  "-Ywarn-unused-import",
  "-Ywarn-inaccessible"
)

lazy val dockerSettings = Seq(
  name := "kafka-d3",
  dockerBaseImage := "openjdk:jre-alpine",
  packageName in Docker := name.value,
  version in Docker := version.value
)

lazy val noPublishSettings = Seq(
  publish := {},
  publishLocal := {},
  publishArtifact := false
)

///////////////////////////////////////////////////////////////////////////////////////////////////
// Dependencies
///////////////////////////////////////////////////////////////////////////////////////////////////

lazy val D = new {

  val Versions = new {
    val avro4s = "3.0.4"
    val cats = "2.0.0"
    val catsEffect = "2.0.0"
    val circe = "0.12.3"
    val consulClient = "1.3.9"
    val fs2 = "2.1.0"
    val http4s = "0.20.15"
    val kafka = "2.3.1"
    val kafkaConfluent = "5.3.1"
    val pureConfig = "0.12.1"
    val scalaJava8 = "0.9.0"

    // Test
    val scalaTest = "3.1.0"

    // Compiler
    val kindProjector = "0.11.0"
  }

  val avro4s = "com.sksamuel.avro4s"             %% "avro4s-core"                 % Versions.avro4s
  val avroSerdes = "io.confluent"                % "kafka-streams-avro-serde"     % Versions.kafkaConfluent
  val cats = "org.typelevel"                     %% "cats-core"                   % Versions.cats
  val catsEffect = "org.typelevel"               %% "cats-effect"                 % Versions.catsEffect
  val circe = "io.circe"                         %% "circe-core"                  % Versions.circe
  val circeGeneric = "io.circe"                  %% "circe-generic"               % Versions.circe
  val consulClient = "com.orbitz.consul"         % "consul-client"                % Versions.consulClient
  val fs2 = "co.fs2"                             %% "fs2-core"                    % Versions.fs2
  val http4sServer = "org.http4s"                %% "http4s-blaze-server"         % Versions.http4s
  val http4sCirce = "org.http4s"                 %% "http4s-circe"                % Versions.http4s
  val http4sClient = "org.http4s"                %% "http4s-blaze-client"         % Versions.http4s
  val http4sDsl = "org.http4s"                   %% "http4s-dsl"                  % Versions.http4s
  val kafkaClient = "org.apache.kafka"           % "kafka-clients"                % Versions.kafka
  val kafkaSchemaRegistryClient = "io.confluent" % "kafka-schema-registry-client" % Versions.kafkaConfluent
  val kafkaStreams = "org.apache.kafka"          %% "kafka-streams-scala"         % Versions.kafka
  val pureConfig = "com.github.pureconfig"       %% "pureconfig"                  % Versions.pureConfig
  val scalaJava8 = "org.scala-lang.modules"      %% "scala-java8-compat"          % Versions.scalaJava8

  // Test
  val scalaTest = "org.scalatest" %% "scalatest" % Versions.scalaTest

  // Compiler
  val kindProjector = "org.typelevel" %% "kind-projector" % Versions.kindProjector cross CrossVersion.full
}

///////////////////////////////////////////////////////////////////////////////////////////////////
// Projects
///////////////////////////////////////////////////////////////////////////////////////////////////

lazy val `kafka-d3` = Project(
  id = "kafka-d3",
  base = file(".")
).settings(moduleName := "kafka-d3")
  .settings(commonSettings)
  .settings(noPublishSettings)
  .aggregate(core)
  .dependsOn(core)

lazy val core = Project(
  id = "core",
  base = file("core")
).settings(moduleName := "core")
  .settings(commonSettings)
  .settings(dockerSettings)
  .settings(Revolver.settings)
  .settings(
    libraryDependencies ++= Seq(
          D.avro4s,
          D.avroSerdes,
          D.cats,
          D.catsEffect,
          D.circe,
          D.circeGeneric,
          D.consulClient,
          D.fs2,
          D.http4sServer,
          D.http4sCirce,
          D.http4sClient,
          D.http4sDsl,
          D.kafkaClient,
          D.kafkaStreams,
          D.kafkaSchemaRegistryClient,
          D.pureConfig,
          D.scalaJava8,
          D.scalaTest % "it,test"
        )
  )
  .configs(IntegrationTest extend Test)
  .settings(Defaults.itSettings)
  .settings(
    fork in IntegrationTest := true,
    parallelExecution in IntegrationTest := false,
    inConfig(IntegrationTest)(ScalafmtPlugin.scalafmtConfigSettings)
  )

lazy val example = Project(
  id = "example",
  base = file("example")
).settings(moduleName := "example")
  .settings(commonSettings)
  .settings(dockerSettings)
  .settings(Revolver.settings)
  .settings(
    libraryDependencies ++= Seq(
          D.cats,
          D.catsEffect,
          D.circe,
          D.circeGeneric,
          D.fs2,
          D.http4sServer,
          D.http4sCirce,
          D.http4sClient,
          D.http4sDsl,
          D.pureConfig
        )
  )
  .dependsOn(core)

///////////////////////////////////////////////////////////////////////////////////////////////////
// Plugins
///////////////////////////////////////////////////////////////////////////////////////////////////

enablePlugins(JavaAppPackaging)
enablePlugins(DockerPlugin)

///////////////////////////////////////////////////////////////////////////////////////////////////
// Commands
///////////////////////////////////////////////////////////////////////////////////////////////////

addCommandAlias("update", ";dependencyUpdates")
addCommandAlias("fcompile", ";scalafmtSbt;compile;test:compile;it:compile")
