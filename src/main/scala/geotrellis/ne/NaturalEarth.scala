package geotrellis.ne

import geotrellis.tools.Resource
import geotrellis.vector._
import geotrellis.vector.io.json.JsonFeatureCollection
import _root_.io.circe._

object NaturalEarth {
  def verison: Int = 4

  def parseAdmin0[T: Decoder]: Vector[Feature[MultiPolygon, T]] = {
    val collection = Resource("admin0.geojson").parseGeoJson[JsonFeatureCollection]
    val ps = collection.getAllPolygonFeatures[T]
    val mps = collection.getAllMultiPolygonFeatures[T]
    ps.map(p => p.mapGeom(MultiPolygon(_))) ++ mps
  }

  def parseAdmin1[T: Decoder]: Vector[Feature[MultiPolygon, T]] = {
    val collection = Resource("admin1.geojson").parseGeoJson[JsonFeatureCollection]
    val ps = collection.getAllPolygonFeatures[T]
    val mps = collection.getAllMultiPolygonFeatures[T]
    ps.map(p => p.mapGeom(MultiPolygon(_))) ++ mps
  }
}
