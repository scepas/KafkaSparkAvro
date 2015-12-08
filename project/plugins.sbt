logLevel := Level.Warn

resolvers ++= Seq(
    "sbt-plugin-releases-repo" at "http://repo.scala-sbt.org/scalasbt/sbt-plugin-releases",
    "sbt-idea-repository" at "http://mpeltonen.github.io/maven/"
)

// https://github.com/cavorite/sbt-avro
addSbtPlugin("com.cavorite" % "sbt-avro" % "0.3.2")