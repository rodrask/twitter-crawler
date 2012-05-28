name := "twitter-crawler"

version := "1.0"

scalaVersion := "2.9.1"

resolvers ++= Seq(
	"twitter4j.org Repository" at "http://twitter4j.org/maven2/",
	"repo.codahale.com" at "http://repo.codahale.com"
	)

libraryDependencies ++= Seq(
	"org.twitter4j" % "twitter4j-core" % "2.2.5",
	"org.twitter4j" % "twitter4j-stream" % "2.2.5",
	"net.databinder" %% "dispatch-http" % "0.8.8",
	"com.codahale" %% "logula" % "2.1.3",
	"org.neo4j" % "neo4j" % "1.7.M01",
	"com.assembla.scala-incubator" %% "graph-core" % "1.4.3")