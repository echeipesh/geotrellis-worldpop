package geotrellis.jobs.ihme

import io.circe._
import io.circe.generic.semiauto._

case class RegionKey(
  location_name: String
)

object RegionKey {
  implicit val regionDecoder: Decoder[RegionKey] = deriveDecoder[RegionKey]
  implicit val regoinEncoder: Encoder[RegionKey] = deriveEncoder[RegionKey]
}