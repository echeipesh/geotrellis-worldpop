package geotrellis.jobs.render

import cats.implicits._
import com.monovore.decline._
import geotrellis.layer._
import geotrellis.raster._
import geotrellis.proj4.WebMercator
import geotrellis.raster.render.ColorRamp
import geotrellis.raster.resample.{Bilinear, NearestNeighbor}
import geotrellis.spark.pyramid.Pyramid
import geotrellis.spark.store.hadoop.SaveToHadoop
import geotrellis.spark.store.s3.SaveToS3
import geotrellis.worldpop.WorldPop
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import software.amazon.awssdk.services.s3.model.ObjectCannedACL


object RenderWorldPopApp extends CommandApp(
  name = getClass.getSimpleName,
  header = "Summarize population within each Admin1 boundary",
  main = {
    val c = Opts.options[String](long = "country", short = "c",
      help = "ISO 3 country code to use for input (cf https://unstats.un.org/unsd/tradekb/knowledgebase/country-code)").
      withDefault(WorldPop.codes.toList.toNel.get)

    val x = Opts.options[String](long = "exclude", short = "x",
      help = "Country code to exclude from input").
      orEmpty

    val o = Opts.option[String](long = "output",
      help = "The path/uri of the ZXY pyraamid")

    val p = Opts.option[Int]("partitions",
      help = "spark.default.parallelism").
      orNone

    (c, x, o, p).mapN { (countriesInclude, excludeCountries, output, numPartitions) =>

      System.setSecurityManager(null)
      val conf = new SparkConf()
        .setIfMissing("spark.master", "local[*]")
        .setAppName(getClass.getSimpleName)
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryo.registrator", "geotrellis.spark.store.kryo.KryoRegistrator")
        .set("spark.task.cpus", "1")
        .set("spark.default.parallelism", numPartitions.getOrElse(64).toString)
        .set("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35")
        .set("spark.network.timeout", "12000s")
        .set("spark.executor.heartbeatInterval", "600s")

      implicit val spark: SparkSession = SparkSession.builder.config(conf).enableHiveSupport.getOrCreate

      try {
        val scheme = ZoomedLayoutScheme(WebMercator)
        val baseZoom = 12
        val layout: LayoutDefinition = scheme.levelForZoom(baseZoom).layout

        val country =
          for (code <- countriesInclude.toList.toArray)
            yield WorldPop.rasterSource(code).get.reprojectToGrid(WebMercator, layout, Bilinear)

        val layer = WorldPop.layerRdd(country, layout, Bilinear, numPartitions.map(new HashPartitioner(_)))
        val pyramid = Pyramid.fromLayerRDD(layer, Some(baseZoom), Some(0), Bilinear)

        pyramid.levels.foreach { case (zoom, tileRdd) =>
          val imageRdd: RDD[(SpatialKey, Array[Byte])] =
            tileRdd.mapValues(_.toArrayTile.renderPng(WorldPopColorMap.blueColorMap).bytes)

          val keyToPath = { k: SpatialKey => s"${output}/${zoom}/${k.col}/${k.row}.png" }
          if (output.startsWith("s3")) {
            SaveToS3(imageRdd, keyToPath, { request =>
              request.toBuilder.acl(ObjectCannedACL.PUBLIC_READ).build()
            })
          } else {
            SaveToHadoop(imageRdd, keyToPath)
          }
        }

      } finally {
        spark.stop()
      }
    }
  })
