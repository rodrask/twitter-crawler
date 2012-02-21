name := "twitter-crawler"

version := "1.0"

scalaVersion := "2.9.1"

resolvers ++= Seq(
	"twitter4j.org Repository" at "http://twitter4j.org/maven2/",
	"fakod-snapshots" at "https://raw.github.com/FaKod/fakod-mvn-repo/master/snapshots")

libraryDependencies ++= Seq(
	"org.twitter4j" % "twitter4j-core" % "2.2.5",
	"org.twitter4j" % "twitter4j-stream" % "2.2.5",
	"org.neo4j" % "neo4j-scala" % "0.2.0-SNAPSHOT")

unmanagedJars in Compile <++= baseDirectory map { base =>
	val baseDirectories = (base / "lib") +++ (base / "neo4j-libs") +++ (base / "storm-libs")
	val customJars = (baseDirectories ** "*.jar")
	customJars.classpath
}