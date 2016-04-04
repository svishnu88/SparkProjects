import AssemblyKeys._

assemblySettings

organization := "com.vishnusparktrials"

name := "sparkprojects"

version := "1.0"

scalaVersion := "2.10.4"

jarName in assembly := "spark-examples.jar"

assemblyOption in assembly ~= { _.copy(includeScala = false) }

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.10" % "1.6.1" ,
  "org.apache.spark" % "spark-sql_2.10" % "1.6.1"  ,
  "org.apache.spark" % "spark-mllib_2.10" % "1.6.1"
)

resolvers ++= Seq(
  "JBoss Repository" at "http://repository.jboss.org/nexus/content/repositories/releases/",
  "Spray Repository" at "http://repo.spray.cc/",
  "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
  "Akka Repository" at "http://repo.akka.io/releases/",
  "Twitter4J Repository" at "http://twitter4j.org/maven2/",
  "Apache HBase" at "https://repository.apache.org/content/repositories/releases",
  "Twitter Maven Repo" at "http://maven.twttr.com/",
  "scala-tools" at "https://oss.sonatype.org/content/groups/scala-tools",
  "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/",
  "Second Typesafe repo" at "http://repo.typesafe.com/typesafe/maven-releases/",
  "Mesosphere Public Repository" at "http://downloads.mesosphere.io/maven",
  Resolver.sonatypeRepo("public")
)

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
{
  case x if x.startsWith("META-INF/ECLIPSEF.RSA") => MergeStrategy.last
  case x if x.startsWith("META-INF/mailcap") => MergeStrategy.last
  case x if x.startsWith("plugin.properties") => MergeStrategy.last
  case x if x.startsWith("Driver.properties") => MergeStrategy.last
  case x if x.startsWith("Logger.class") => MergeStrategy.last
  case x if x.startsWith("Driver.properties") => MergeStrategy.last
  case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.first
  case PathList("org", "apache", xs @ _*) => MergeStrategy.first
  case x => old(x)
  case _ => MergeStrategy.first
}
}

