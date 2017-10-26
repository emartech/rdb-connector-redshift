name := "rdb-connector-redshift"

version := "0.1"

scalaVersion := "2.12.4"

lazy val ItTest = config("it") extend Test

lazy val root = (project in file("."))
  .configs(ItTest)
  .settings(
    inConfig(ItTest)(Seq(Defaults.itSettings: _*))
  )



resolvers += "Amazon"  at "http://redshift-maven-repository.s3-website-us-east-1.amazonaws.com/release"
resolvers += "jitpack" at "https://jitpack.io"

libraryDependencies ++= {
  val scalaTestV = "3.0.1"
  Seq(
    "com.github.emartech" % "rdb-connector-common"  % "-SNAPSHOT" changing(),
    "com.typesafe.slick"  %% "slick"                % "3.2.0",
    "com.amazon.redshift" %  "redshift-jdbc42"      % "1.2.8.1005",
    "org.scalatest"       %% "scalatest"            % scalaTestV  % "test"
  )
}