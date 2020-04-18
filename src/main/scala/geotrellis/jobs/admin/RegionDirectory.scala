package geotrellis.jobs.admin

import geotrellis.vector._
import geotrellis.layer._

case class RegionDirectory(regions: Vector[Feature[Polygon, Admin1Key]]) {
  // we can't deal with anti-meridian intersection ... that's sad
  val mm1 = LineString(Point(180,-90), Point(180,90))
  val mm2 = LineString(Point(-180,-90), Point(-180,90))

  def findIntersecting(aoi: Geometry): Iterable[Feature[Polygon, Admin1Key]] = {
    regions
      .filter(_.geom.intersects(aoi))
      .filterNot(_.geom.intersects(mm1))
      .filterNot(_.geom.intersects(mm2))
  }
}
