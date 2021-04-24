lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "org.schtch",
      scalaVersion := "2.11.0",

      libraryDependencies ++= Seq(
//        "org.scala-lang.modules" %% "scala-parser-combinators" % "1.1.2",
        "org.apache.spark" %% "spark-core" % "2.2.1",
        "org.apache.spark" %% "spark-streaming" % "2.2.1",
      )
    )),
    name := "spark-twosources",
)
