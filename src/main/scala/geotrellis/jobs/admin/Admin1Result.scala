package geotrellis.jobs.admin

import geotrellis.proj4.util.UTM
import geotrellis.proj4.{LatLng, WebMercator}
import geotrellis.raster.summary.Statistics
import io.circe._
import io.circe.generic.semiauto._
import geotrellis.vector._

case class Admin1Result(
  adm1_code: String,
  adm0_a3: String,
  population: Long,
  density: Double,
  stats: Option[Statistics[Double]]
) {
  def csvLine: String = List(
    adm1_code,
    adm0_a3,
    population.toLong.toString,
    density.toLong.toString,
    stats.map(_.zmin.toString).getOrElse("null"),
    stats.map(_.zmax.toString).getOrElse("null"),
    stats.map(_.mean.toString).getOrElse("null"),
    stats.map(_.median.toString).getOrElse("null"),
    stats.map(_.mode.toString).getOrElse("null"),
    stats.map(_.stddev.toString).getOrElse("null")
  ).mkString(",")
}

object Admin1Result {
  implicit val resultDecoder: Decoder[Admin1Result] = deriveDecoder[Admin1Result]
  implicit val resultEncoder: Encoder[Admin1Result] = deriveEncoder[Admin1Result]

  implicit val statDecoder: Decoder[Statistics[Double]] = deriveDecoder[Statistics[Double]]
  implicit val statEncoder: Encoder[Statistics[Double]] = deriveEncoder[Statistics[Double]]

  def regionAreaKM(mp: MultiPolygon): Double = {
    for (poly <- mp.polygons) yield {
      val center = poly.getCentroid
      val utmCrs = UTM.getZoneCrs(center.getX, center.getY)
      poly.reproject(LatLng, utmCrs).getArea / 1000 // UTM unit is meter
    }
  }.sum

  def fromPopulationSummary(f: Feature[MultiPolygon, Admin1Key], summary: Option[PopulationSummary]): Admin1Result = {
    val area = Admin1Result.regionAreaKM(f.geom)
    summary match {
      case Some(sum) =>
        val density = sum.population / area
        Admin1Result(f.data.adm1_code, f.data.adm0_a3, sum.population.toLong, density, sum.distribution.statistics())

      case None =>
        Admin1Result(f.data.adm1_code, f.data.adm0_a3, 0L, area.toLong, None)

    }
  }

  val csvHeader: String = "adm1_code,adm0_a3,population,density,min,max,mean,median,mode,stddev"
}