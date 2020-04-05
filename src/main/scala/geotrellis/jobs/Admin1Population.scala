package geotrellis.jobs

import java.io.PrintWriter
import java.net.URI

import cats._
import cats.implicits._
import com.monovore.decline._
import geotrellis.layer._
import geotrellis.vector._
import geotrellis.raster._
import geotrellis.raster.summary.polygonal._
import geotrellis.raster.summary.polygonal.visitors._
import cats.data.NonEmptyList
import geotrellis.ne.NaturalEarth
import geotrellis.raster.resample.NearestNeighbor
import geotrellis.raster.summary.types.SumValue
import geotrellis.raster.{GridExtent, RasterRegion}
import geotrellis.worldpop.WorldPop
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._


/** Total population per Admin1 boundary */
object Admin1Population extends CommandApp(
  name = "Admin1 Population",
  header = "Summarize population within each Admin1 boundary",
  main = {
    val c = Opts.options[String](long = "country", short = "c",
      help = "ISO 3 country code to use for input (cf https://unstats.un.org/unsd/tradekb/knowledgebase/country-code)").
      withDefault(WorldPop.codes.toList.toNel.get)

    val x = Opts.options[String](long = "exclude", short = "x",
      help = "Country code to exclude from input").
      orEmpty

    val o = Opts.option[String](long = "output",
      help = "The path/uri of the summary JSON file")

    val p = Opts.option[Int]("partitions",
      help = "spark.default.parallelism").
      orNone

    (c, x, o, p).mapN { (countriesInclude, excludeCountries, output, numPartitions) =>

      System.setSecurityManager(null)
      val conf = new SparkConf()
        .setIfMissing("spark.master", "local[*]")
        .setAppName("Admin1Population")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryo.registrator", "geotrellis.spark.store.kryo.KryoRegistrator")
        .set("spark.task.cpus", "1")
        .set("spark.default.parallelism", numPartitions.getOrElse(123).toString)
        .set("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35")
        .set("spark.network.timeout", "12000s")
        .set("spark.executor.heartbeatInterval", "600s")

      implicit val spark: SparkSession = SparkSession.builder.config(conf).enableHiveSupport.getOrCreate

      try {
        val regions = spark.sparkContext.broadcast(
          RegionDirectory(
            for {
              mpFeature <- NaturalEarth.parseAdmin1[Admin1Key]
              polygon <- mpFeature.geom.polygons
            } yield Feature(polygon, mpFeature.data)
          )
        )

        val job = new Admin1Population(countriesInclude, regions, numPartitions.getOrElse(123))
        val result: Map[Admin1Key, Double] =
          job.result
            .collect
            .toMap
            .map { case (adm, sum) =>
              adm -> sum.toOption.map(_.value).getOrElse(Double.NaN)
            }

        // Output
        val features = NaturalEarth.parseAdmin1[Admin1Key].map(f => f.data.adm1_code -> f).toMap

        val conf = spark.sparkContext.hadoopConfiguration
        val fs = FileSystem.get(new URI(output), conf)
        val pw = new PrintWriter(fs.create(new Path(output)))
        try {
          for ((adm, pop) <- result) {
            val f = features(adm.adm1_code)
            val area = Admin1Result.regionAreaKM(f.geom)
            val result = Admin1Result(adm.adm1_code, adm.adm0_a3, pop.toLong, area.toLong)
            val json = Feature(f.geom, result).toGeoJson
            pw.println(json)
          }
        }
        finally {
          pw.close()
          fs.close()
        }

      } finally {
        spark.stop()
      }
    }
  }
)

class Admin1Population(
  @transient val countryCodes: NonEmptyList[String],
  regions: Broadcast[RegionDirectory],
  @transient val numPartitions: Int
)(implicit
  spark: SparkSession
) extends Serializable {
  // this is minimum chunk of work that may intersect multiple admin regions and raster regions
  val jobGrid = LayoutDefinition[Long](
    grid = GridExtent[Long](Extent(-180, -90, 180, 90), CellSize(8.3333333E-4,8.3333333E-4)),
    tileSize = 512
  )

  // All the countries, value (CountryCode, RasterRegion)
  val regionRdd: RDD[(SpatialKey, String)] = spark.sparkContext
    .parallelize(countryCodes.toList, countryCodes.length)
    .flatMap { code =>
      val source = WorldPop.rasterSource(code).get.resampleToGrid(jobGrid, NearestNeighbor)
      val layoutTileSource: LayoutTileSource[SpatialKey] = LayoutTileSource.spatial(source, jobGrid)
      layoutTileSource.keys.map { key =>
        key -> code //layoutTileSource.rasterRegionForKey(key).get
      }
    }.repartition(numPartitions)


  val result: RDD[(Admin1Key, PolygonalSummaryResult[SumValue])] =
    regionRdd
      .flatMap { case (key, code) =>
        val source = WorldPop.rasterSource(code).get.resampleToGrid(jobGrid, NearestNeighbor)
        val layoutTileSource: LayoutTileSource[SpatialKey] = LayoutTileSource.spatial(source, jobGrid)
        val region = layoutTileSource.rasterRegionForKey(key).get

        println(s"Key: ${key}")
        for {
          raster <- region.raster.map(_.mapTile(_.band(0))).toList
          feature <- regions.value.findIntersecting(key.extent(jobGrid))
        } yield {
          println(s"Summary: ${feature.data.adm1_code}")
          feature.data -> raster.polygonalSummary(feature.geom, SumVisitor)
        }
      }
      .reduceByKey(_ combine _)
}
