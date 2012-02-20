name := "twitter-crawler"

version := "1.0"

scalaVersion := "2.9.1"

resolvers ++= Seq("maven_central"  at "http://repo1.maven.org/maven2/",
	"twitter4j.org Repository" at "http://twitter4j.org/maven2/",
	"clojars.org" at "http://clojars.org/repo",
	"fakod-snapshots" at "https://raw.github.com/FaKod/fakod-mvn-repo/master/snapshots")

libraryDependencies ++= Seq(
	"org.twitter4j" % "twitter4j-core" % "2.2.5",
	"org.twitter4j" % "twitter4j-stream" % "2.2.5",
	"storm" % "storm" % "0.6.2",
	"org.neo4j" % "neo4j" % "1.6.M03",
	"org.neo4j" % "neo4j-scala" % "0.2.0-SNAPSHOT")
