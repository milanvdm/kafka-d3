addSbtCoursier
addSbtPlugin("com.geirsson"     % "sbt-scalafmt"        % "1.6.0-RC4")
addSbtPlugin("io.spray"         % "sbt-revolver"        % "0.9.1")
addSbtPlugin("com.timushev.sbt" % "sbt-updates"         % "0.4.0")
addSbtPlugin("com.scalapenos"   % "sbt-prompt"          % "1.0.2")
addSbtPlugin("com.typesafe.sbt" % "sbt-native-packager" % "1.3.18")

addCompilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.0-M4")

classpathTypes += "maven-plugin"
