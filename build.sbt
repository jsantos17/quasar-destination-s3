import scala.collection.Seq

homepage in ThisBuild := Some(url("https://github.com/slamdata/quasar-destination-s3"))

scmInfo in ThisBuild := Some(ScmInfo(
  url("https://github.com/slamdata/quasar-destination-s3"),
  "scm:git@github.com:slamdata/quasar-destination-s3.git"))

val ArgonautVersion = "6.2.3"
val AsyncBlobstoreVersion = "2.1.0"
val AwsSdkVersion = "2.9.1"
val AwsV1SdkVersion = "1.11.634"
val Fs2Version = "2.1.0"
val MonixVersion = "3.0.0"
val SpecsVersion = "4.8.3"

// Include to also publish a project's tests
lazy val publishTestsSettings = Seq(
  publishArtifact in (Test, packageBin) := true)

lazy val QuasarVersion = IO.read(file("./quasar-version")).trim

lazy val root = project
  .in(file("."))
  .settings(noPublishSettings)
  .aggregate(core)
  .enablePlugins(AutomateHeaderPlugin)

lazy val core = project
  .in(file("core"))
  .settings(name := "quasar-destination-s3")
  .settings(
    performMavenCentralSync := false,
    quasarPluginName := "s3-dest",
    quasarPluginQuasarVersion := QuasarVersion,
    quasarPluginDestinationFqcn := Some("quasar.destination.s3.S3DestinationModule$"),
    quasarPluginDependencies ++= Seq(
      "io.argonaut"  %% "argonaut" % ArgonautVersion,
      "co.fs2" %% "fs2-core" % Fs2Version,
      "com.slamdata" %% "async-blobstore-core" % AsyncBlobstoreVersion,
      "com.slamdata" %% "async-blobstore-s3" % AsyncBlobstoreVersion,
      "software.amazon.awssdk" % "netty-nio-client" % AwsSdkVersion,
      "software.amazon.awssdk" % "s3" % AwsSdkVersion,
      // We depend on both v1 and v2 S3 SDKs because of this ticket:
      // https://github.com/aws/aws-sdk-java-v2/issues/272
      // Depending on both is the recommended workaround
      "com.amazonaws" % "aws-java-sdk-s3" % AwsV1SdkVersion),
    libraryDependencies ++= Seq(
      "org.specs2" %% "specs2-core" % SpecsVersion % Test,
      "com.slamdata" %% "quasar-foundation" % QuasarVersion,
      "com.slamdata" %% "quasar-foundation" % QuasarVersion % Test classifier "tests",
      "org.specs2" %% "specs2-scalacheck" % SpecsVersion % Test,
      "org.specs2" %% "specs2-scalaz" % SpecsVersion % Test),
    publishAsOSSProject := true)
  .enablePlugins(AutomateHeaderPlugin, QuasarPlugin)
