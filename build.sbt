name := "customer-table-updater"

version := "0.1"

scalaVersion := "2.12.4"


scalacOptions ++= Seq(
  "-deprecation",
  "-feature",
  "-unchecked",
  "-encoding", "UTF-8",
  "-target:jvm-1.8"
)
libraryDependencies ++= Seq(
        "com.github.tototoshi" %% "scala-csv" % "1.3.8",
      "org.typelevel"                %% "cats-core"                                     % "2.9.0",
"com.github.pureconfig"        %% "pureconfig"                                    % "0.12.1",
  "commons-codec" % "commons-codec" % "1.15",
  "org.slf4j" % "slf4j-api" % "1.7.32",
  "org.slf4j" % "slf4j-simple" % "1.7.32",
  "com.typesafe.akka" %% "akka-stream"          % "2.6.0",
 "com.github.etaty"             %% "rediscala"                                     % "1.9.0"
)


mainClass in Compile := Some("RedisLatLngAdder")

