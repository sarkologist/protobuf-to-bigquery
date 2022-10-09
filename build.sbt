name := "ProtobufToBigquery" 
version := "0.1.0" 
scalaVersion := "2.13.9"


libraryDependencies += "com.google.cloud" % "google-cloud-bigquery" % "1.116.0"
libraryDependencies += "org.scalaz" %% "scalaz-core" % "7.3.6"
libraryDependencies += "org.scalactic" %% "scalactic" % "3.2.14"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.14" % "test"
libraryDependencies += "org.scalatestplus" %% "scalacheck-1-17" % "3.2.14.0" % "test"
libraryDependencies += "com.thesamet.scalapb" %% "compilerplugin" % "0.10.11" % "test"
libraryDependencies += "com.github.os72" % "protoc-jar" % "3.11.4" % "test"