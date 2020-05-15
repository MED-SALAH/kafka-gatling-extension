package io.gatling

import java.nio.file.Path

import io.gatling.commons.util.PathHelper._

object IDEPathHelper {

  lazy val gatlingConfUrl: Path = getClass.getClassLoader.getResource("gatling.conf").toURI
  lazy val projectRootDir = gatlingConfUrl.ancestor(3)

  val mavenSourcesDirectory = projectRootDir / "src" / "main" / "scala"
  val mavenResourcesDirectory = projectRootDir / "src" / "test" / "resources"
  val mavenTargetDirectory = projectRootDir / "target"
  val mavenBinariesDirectory = mavenTargetDirectory / "test-classes"

  val dataDirectory = mavenResourcesDirectory / "data"
  val bodiesDirectory = mavenResourcesDirectory / "bodies"

  val resultsDirectory = mavenTargetDirectory / "io/gatling"
}