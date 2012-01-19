
name := "zookeeper-client"

version := "1.0.0"

scalaVersion := "2.9.1"

libraryDependencies ++= Seq(
    "org.apache.zookeeper" % "zookeeper" % "3.3.3" excludeAll(
        ExclusionRule(organization = "com.sun.jdmk"),
        ExclusionRule(organization = "com.sun.jmx"),
        ExclusionRule(organization = "javax.jms")
    ),
    "org.specs2" %% "specs2" % "1.7" % "test",
    "org.specs2" %% "specs2-scalaz-core" % "6.0.1" % "test"
  )

