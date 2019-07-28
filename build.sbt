import com.scalapenos.sbt.prompt._

///////////////////////////////////////////////////////////////////////////////////////////////////
// Settings
///////////////////////////////////////////////////////////////////////////////////////////////////

lazy val commonSettings = Seq(
  organization := "me.milanvdm",
  scalaVersion := "2.12.8",
  resolvers ++= Seq(
        "Typesafe Releases" at "http://repo.typesafe.com/typesafe/maven-releases/",
        "confluent.io" at "http://packages.confluent.io/maven/"
      ),
  scalafmtOnCompile := true,
  incOptions := incOptions.value.withLogRecompileOnMacro(false),
  scalacOptions ++= commonScalacOptions,
  fork in Test := true,
  parallelExecution in Test := false,
  testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oDF"),
  libraryDependencies ++= Seq(
        compilerPlugin(D.kindProjector),
        compilerPlugin(D.macroParadise)
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
    val avro4s = "2.0.4"
    val cats = "1.6.1"
    val catsEffect = "1.3.1"
    val catsPar = "0.2.1"
    val circe = "0.11.1"
    val fs2 = "1.0.5"
    val http4s = "0.20.6"
    val kafka = "2.3.0"
    val kafkaConfluent = "5.3.0"
    val pureConfig = "0.11.1"
    val scalaJava8 = "0.9.0"

    // Test
    val scalaTest = "3.0.8"

    // Compiler
    val kindProjector = "0.9.10"
    val macroParadise = "2.1.1"
  }

  val avro4s = "com.sksamuel.avro4s"             %% "avro4s-core"                 % Versions.avro4s
  val avroSerdes = "io.confluent"                % "kafka-streams-avro-serde"     % Versions.kafkaConfluent
  val cats = "org.typelevel"                     %% "cats-core"                   % Versions.cats
  val catsEffect = "org.typelevel"               %% "cats-effect"                 % Versions.catsEffect
  val catsPar = "io.chrisdavenport"              %% "cats-par"                    % Versions.catsPar
  val circe = "io.circe"                         %% "circe-core"                  % Versions.circe
  val circeGeneric = "io.circe"                  %% "circe-generic"               % Versions.circe
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
  val kindProjector = "org.spire-math"  %% "kind-projector" % Versions.kindProjector
  val macroParadise = "org.scalamacros" %% "paradise"       % Versions.macroParadise cross CrossVersion.full
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
          D.catsPar,
          D.circe,
          D.circeGeneric,
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
