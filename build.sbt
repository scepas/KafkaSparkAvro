
name := "KafkaSparkAvro"

version := "1.0"

scalaVersion := "2.10.5"

seq(sbtavro.SbtAvro.avroSettings : _*)

// Configure the desired Avro version.  sbt-avro automatically injects a libraryDependency.
(version in avroConfig) := "1.7.7"

// Look for *.avsc etc. files in src/test/avro
(sourceDirectory in avroConfig) <<= (sourceDirectory in Compile)(_ / "avro")

(stringType in avroConfig) := "String"

resolvers ++= Seq(
    "typesafe-repository" at "http://repo.typesafe.com/typesafe/releases/",
    "cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
    "snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
    "releases" at "http://oss.sonatype.org/content/repositories/releases"
)

val sparkVersion = "1.5.0-cdh5.5.0"
val bijectionVersion = "0.8.1"

libraryDependencies ++= Seq(
    "com.twitter" %% "bijection-core" % bijectionVersion,
    "com.twitter" %% "bijection-avro" % bijectionVersion,
    "org.apache.kafka" % "kafka_2.10" % "0.8.2.1"
        exclude("javax.jms", "jms")
        exclude("com.sun.jdmk", "jmxtools")
        exclude("com.sun.jmx", "jmxri")
        exclude("org.slf4j", "slf4j-simple")
        exclude("log4j", "log4j")
        exclude("org.apache.zookeeper", "zookeeper")
        exclude("com.101tec", "zkclient"),
    "org.apache.spark" %% "spark-core" % sparkVersion
        exclude("org.apache.zookeeper", "zookeeper")
        exclude("org.slf4j", "slf4j-api")
        exclude("org.slf4j", "slf4j-log4j12")
        exclude("org.slf4j", "jul-to-slf4j")
        exclude("org.slf4j", "jcl-over-slf4j")
        exclude("log4j", "log4j"),
    "org.apache.spark" %% "spark-streaming-kafka" % sparkVersion
        exclude("org.apache.zookeeper", "zookeeper"),
    "org.apache.spark" %% "spark-streaming" % sparkVersion
        exclude("org.apache.zookeeper", "zookeeper"),
    "com.101tec" % "zkclient" % "0.4"
        exclude("org.apache.zookeeper", "zookeeper"),
    "org.apache.curator" % "curator-test" % "2.4.0"
        exclude("org.jboss.netty", "netty")
        exclude("org.slf4j", "slf4j-log4j12"),
    "commons-io" % "commons-io" % "2.4",
    "commons-logging" % "commons-logging" % "1.2",
    "org.apache.commons" % "commons-pool2" % "2.3",
    // Logback with slf4j facade
    "ch.qos.logback" % "logback-classic" % "1.1.2",
    // Test dependencies
    "org.scalatest" %% "scalatest" % "2.2.4" % "test",
    "org.mockito" % "mockito-all" % "1.9.5" % "test",
    "org.scalacheck" %% "scalacheck" % "1.12.5" % "test"
)

javaOptions ++= Seq(
    "-Xms256m",
    "-Xmx512m",
    "-XX:+UseG1GC",
    "-XX:MaxGCPauseMillis=20",
    "-XX:MaxPermSize=1G",
    "-XX:InitiatingHeapOccupancyPercent=35",
    "-Djava.awt.headless=true",
    "-Djava.net.preferIPv4Stack=true")

javacOptions in Compile ++= Seq(
    "-source", "1.7",
    "-target", "1.7",
    "-Xlint:unchecked",
    "-Xlint:deprecation")

scalacOptions ++= Seq(
    "-target:jvm-1.7",
    "-encoding", "UTF-8"
)

scalacOptions in Compile ++= Seq(
    "-deprecation", // Emit warning and location for usages of deprecated APIs.
    "-feature",  // Emit warning and location for usages of features that should be imported explicitly.
    "-unchecked", // Enable additional warnings where generated code depends on assumptions.
    "-Xlint", // Enable recommended additional warnings.
    "-Ywarn-adapted-args", // Warn if an argument list is modified to match the receiver.
    "-Ywarn-dead-code",
    "-Ywarn-value-discard" // Warn when non-Unit expression results are unused.
)

// Write test results to file in JUnit XML format
testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-u", "target/test-reports/junitxml")

testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-o")

testOptions in Test += Tests.Argument(TestFrameworks.ScalaCheck, "-maxSize", "10000")