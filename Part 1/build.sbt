import org.apache.logging.log4j.core.config.composite.MergeStrategy
import sun.security.tools.PathList

name := "Project"

version := "1.0"


scalaVersion := "2.12.0"


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.6.2",
  "org.apache.spark" %% "spark-sql" % "1.6.2"
)