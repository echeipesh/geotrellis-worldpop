package geotrellis.jobs

import geotrellis.proj4.util.UTM
import geotrellis.proj4.{LatLng, WebMercator}
import io.circe._
import io.circe.generic.semiauto._
import geotrellis.vector._

case class Admin1Result(
  adm1_code: String,
  adm0_a3: String,
  population: Long,
  area_km: Long
)

object Admin1Result {
  implicit val fooDecoder: Decoder[Admin1Result] = deriveDecoder[Admin1Result]
  implicit val fooEncoder: Encoder[Admin1Result] = deriveEncoder[Admin1Result]

  def regionAreaKM(mp: MultiPolygon): Double = {
    for (poly <- mp.polygons) yield {
      val center = poly.getCentroid
      val utmCrs = UTM.getZoneCrs(center.getX, center.getY)
      poly.reproject(LatLng, utmCrs).getArea / 1000 // UTM unit is meter
    }
  }.sum
}