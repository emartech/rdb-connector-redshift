name := "rdb-connector-redshift"

version := "0.1"

scalaVersion := "2.12.3"

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
  val slickV = "3.2.0"
  Seq(
    "com.github.emartech" %  "rdb-connector-common" % "67bc7a5e27",
    "com.typesafe.slick"  %% "slick"                % slickV,
    "com.typesafe.slick"  %% "slick-hikaricp"       % slickV,
    "com.amazon.redshift" %  "redshift-jdbc42"      % "1.2.8.1005",
    "org.scalatest"       %% "scalatest"            % scalaTestV   % Test,
    "com.typesafe.akka"   %% "akka-stream-testkit"  % "2.5.6"      % Test,
    "com.github.emartech" %  "rdb-connector-test"   % "299379b749" % Test,
    "com.typesafe.akka"   %% "akka-http-spray-json" % "10.0.7"     % Test,
    "org.mockito"         %  "mockito-core"         % "2.11.0"     % Test
  )
}