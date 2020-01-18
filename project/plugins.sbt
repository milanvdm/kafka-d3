addSbtCoursier
addSbtPlugin("org.scalameta"    % "sbt-scalafmt"        % "2.3.0")
addSbtPlugin("io.spray"         % "sbt-revolver"        % "0.9.1")
addSbtPlugin("com.scalapenos"   % "sbt-prompt"          % "1.0.2")
addSbtPlugin("com.typesafe.sbt" % "sbt-native-packager" % "1.5.2")

addCompilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1")

classpathTypes += "maven-plugin"
