name := "twitter-crawler"

version := "1.0"

scalaVersion := "2.9.1"

resolvers ++= Seq(
	"twitter4j.org Repository" at "http://twitter4j.org/maven2/")

libraryDependencies ++= Seq(
	"org.twitter4j" % "twitter4j-core" % "2.2.5",
	"org.twitter4j" % "twitter4j-stream" % "2.2.5",
	"redis.clients" % "jedis" % "2.0.0")

unmanagedJars in Compile <++= baseDirectory map { base =>
	val baseDirectories = (base / "lib") +++ (base / "neo4j-libs") +++ (base / "storm-libs")
	val customJars = (baseDirectories ** "*.jar")
	customJars.classpath
}