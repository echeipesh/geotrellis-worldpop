package geotrellis.jobs

import io.circe._
import io.circe.generic.semiauto._

case class Admin1Key(
  adm1_code: String,
  adm0_a3: String
)

object Admin1Key {
  implicit val fooDecoder: Decoder[Admin1Key] = deriveDecoder[Admin1Key]
  implicit val fooEncoder: Encoder[Admin1Key] = deriveEncoder[Admin1Key]
}