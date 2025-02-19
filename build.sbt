name := "geotrellis-worldpop"
version := "2.1"
scalaVersion := "2.12.11"
organization := "geotrellis"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % Version.spark % Provided,
  "org.apache.spark" %% "spark-hive" % Version.spark % Provided,
  "org.locationtech.geotrellis" %% "geotrellis-s3" % Version.geotrellis,
  "org.locationtech.geotrellis" %% "geotrellis-spark" % Version.geotrellis,
  "org.locationtech.geotrellis" %% "geotrellis-s3-spark" % Version.geotrellis,
  "org.log4s" %% "log4s" % "1.8.2",
  "com.monovore" %% "decline" % "1.2.0",
  "org.scalatest" %% "scalatest" % "3.0.8" % Test
)

Test / fork := true
Test / connectInput := true
outputStrategy := Some(StdoutOutput)

test in assembly := {}

assemblyShadeRules in assembly := {
  val shadePackage = "geotrellis.sdg.shaded"
  Seq(
    ShadeRule.rename("com.fasterxml.jackson.**" -> s"$shadePackage.com.fasterxml.jackson.@1")
      .inLibrary("com.networknt" % "json-schema-validator" % "0.1.7").inAll,
    ShadeRule.rename("org.apache.avro.**" -> s"$shadePackage.org.apache.avro.@1")
      .inLibrary("com.azavea.geotrellis" %% "geotrellis-spark" % Version.geotrellis).inAll
  )
}

assemblyMergeStrategy in assembly := {
  case "reference.conf" => MergeStrategy.concat
  case "application.conf" => MergeStrategy.concat
  case PathList("META-INF", "services", "org.opengis.referencing.crs.CRSAuthorityFactory") => MergeStrategy.concat
  case PathList("META-INF", xs@_*) =>
    xs match {
      case ("MANIFEST.MF" :: Nil) => MergeStrategy.discard
      // Concatenate everything in the services directory to keep GeoTools happy.
      case ("services" :: _ :: Nil) =>
        MergeStrategy.concat
      // Concatenate these to keep JAI happy.
      case ("javax.media.jai.registryFile.jai" :: Nil) | ("registryFile.jai" :: Nil) | ("registryFile.jaiext" :: Nil) =>
        MergeStrategy.concat
      case (name :: Nil) => {
        // Must exclude META-INF/*.([RD]SA|SF) to avoid "Invalid signature file digest for Manifest main attributes" exception.
        if (name.endsWith(".RSA") || name.endsWith(".DSA") || name.endsWith(".SF"))
          MergeStrategy.discard
        else
          MergeStrategy.first
      }
      case _ => MergeStrategy.first
    }
  case _ => MergeStrategy.first
}

sparkInstanceCount          := 80
sparkMasterType             := "m4.4xlarge"
sparkCoreType               := "m4.4xlarge"
sparkMasterPrice            := Some(1.00)
sparkCorePrice              := Some(1.00)
sparkEmrRelease             := "emr-6.0.0"
sparkAwsRegion              := "us-east-1"
sparkSubnetId               := Some("subnet-4f553375")
sparkS3JarFolder            := s"s3://geotrellis-test/jars/${Environment.user}"
sparkS3LogUri               := Some(s"s3://geotrellis-test/logs/${Environment.user}/")
sparkClusterName            := s"geotrellis-worldpop-${Environment.user}"
sparkEmrServiceRole         := "EMR_DefaultRole"
sparkInstanceRole           := "EMR_EC2_DefaultRole"
sparkEmrApplications        := Seq("Spark", "Zeppelin", "Ganglia")
sparkMasterEbsSize          := Some(64)
sparkCoreEbsSize            := Some(64)
sparkJobFlowInstancesConfig := sparkJobFlowInstancesConfig.value.withEc2KeyName("geotrellis-emr")

import com.amazonaws.services.elasticmapreduce.model.Application
sparkRunJobFlowRequest      := sparkRunJobFlowRequest.value.withApplications(
  Seq("Spark", "Ganglia").map(a => new Application().withName(a)):_*
)

import sbtlighter.EmrConfig
sparkEmrConfigs := Seq(
  EmrConfig("spark").withProperties(
    "maximizeResourceAllocation" -> "true"),
  EmrConfig("spark-defaults").withProperties(
  "spark.driver.maxResultSize" -> "2G",
  "spark.executor.maxResultSize" -> "2G",
  "spark.dynamicAllocation.enabled" -> "true",
  "spark.shuffle.service.enabled" -> "true",
  "spark.shuffle.compress" -> "true",
  "spark.shuffle.spill.compress" -> "true",
  "spark.rdd.compress" -> "true",
  "spark.executor.extraJavaOptions" -> "-XX:+UseParallelGC"),
  EmrConfig("yarn-site").withProperties(
    "yarn.resourcemanager.am.max-attempts" -> "1",
    "yarn.nodemanager.vmem-check-enabled" -> "false",
    "yarn.nodemanager.pmem-check-enabled" -> "false"
  )
)

initialCommands in console :=
"""
import geotrellis.proj4._
import geotrellis.vector._
import geotrellis.raster._
import geotrellis.layer._
"""