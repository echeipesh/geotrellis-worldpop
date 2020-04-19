package geotrellis.jobs.ihme

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
import geotrellis.raster.summary.polygonal.visitors.SumVisitor.TileSumVisitor
import geotrellis.tools.Resource
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

import java.nio.charset.StandardCharsets
import java.io.OutputStreamWriter


/** Total population per Admin1 boundary */
object IhmePopulationApp extends CommandApp(
  name = "Admin1 Population",
  header = "Summarize population within each Admin1 boundary",
  main = {
    val c = Opts.options[String](long = "country", short = "c",
      help = "ISO 3 country code to use for input (cf https://unstats.un.org/unsd/tradekb/knowledgebase/country-code)").
      withDefault(WorldPop.codes.toList.toNel.get)

    val x = Opts.options[String](long = "exclude", short = "x",
      help = "Country code to exclude from input").
      orEmpty

    val r = Opts.option[String](long = "regions",
      help = "Name of resource GeoJSON file containing the regions").
      withDefault("ihme-region-data.geojson")

    val o = Opts.option[String](long = "output",
      help = "The path/uri of the summary CSV file")

    val p = Opts.option[Int]("partitions",
      help = "spark.default.parallelism").
      orNone

    (c, x, r, o, p).mapN { (countriesInclude, excludeCountries, regionsFile, output, numPartitions) =>

      System.setSecurityManager(null)
      val conf = new SparkConf()
        .setIfMissing("spark.master", "local[*]")
        .setAppName("IhmePopulation")
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
          Resource.parseMultiPolygonFeatures[RegionKey](regionsFile)
        )

        val job = new IhmePopulationApp(countriesInclude, regions, numPartitions.getOrElse(123))
        val result: Array[(RegionKey, PolygonalSummaryResult[SumValue])] =
          job.result.collect

        // Output

        val conf = spark.sparkContext.hadoopConfiguration
        val fs = FileSystem.get(new URI(output), conf)
        val out = new OutputStreamWriter(fs.create(new Path(output)), StandardCharsets.UTF_8)
        val csv = new PrintWriter(out)
        csv.println("location_name,population")
        try {
          for ((region, sum) <- result) {
            sum.toOption match {
              case Some(sv) =>
                val line = s"${region.location_name},${sv.value.toLong}"
                println(line)
                csv.println(line)

              case None =>
                println(s"${region.location_name} didn't intersect any rasters")
            }
          }
        }
        finally {
          csv.close()
          fs.close()
        }

      } finally {
        spark.stop()
      }
    }
  }
)

class IhmePopulationApp(
  @transient val countryCodes: NonEmptyList[String],
  regions: Broadcast[Vector[Feature[MultiPolygon, RegionKey]]],
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


  val result: RDD[(RegionKey, PolygonalSummaryResult[SumValue])] =
    regionRdd
      .flatMap { case (key, code) =>
        val source = WorldPop.rasterSource(code).get.resampleToGrid(jobGrid, NearestNeighbor)
        val layoutTileSource: LayoutTileSource[SpatialKey] = LayoutTileSource.spatial(source, jobGrid)
        val region = layoutTileSource.rasterRegionForKey(key).get
        val bbox = key.extent(jobGrid)

        println(s"Key: ${key}")
        for {
          feature <- regions.value.filter(_.geom.intersects(bbox)).toList
          raster <- region.raster.map(_.mapTile(_.band(0))).toList
        } yield {
          println(s"Summary: ${feature.data}")
          feature.data -> raster.polygonalSummary(feature.geom, new TileSumVisitor)
        }
      }
      .reduceByKey(_ combine _)
}
