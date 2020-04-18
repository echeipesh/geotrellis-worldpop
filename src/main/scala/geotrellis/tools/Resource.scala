package geotrellis.tools

import java.io._
import java.net.{URI, URL}
import java.nio.charset.StandardCharsets
import org.apache.commons.io.IOUtils
import java.io.StringWriter
import org.apache.orc.DataMask.Standard

import geotrellis.vector._
import geotrellis.vector.io.json.JsonFeatureCollection
import _root_.io.circe._

object Resource {
  def apply(name: String): String = {
    val stream: InputStream = getClass.getResourceAsStream(s"/$name")
    try {
      val writer = new StringWriter
      IOUtils.copy(stream, writer, StandardCharsets.UTF_8)
      writer.toString
    } finally {
      stream.close
    }
  }

  def parseMultiPolygonFeatures[T: Decoder](name: String): Vector[Feature[MultiPolygon, T]] = {
    val collection = Resource(name).parseGeoJson[JsonFeatureCollection]
    val ps = collection.getAllPolygonFeatures[T]
    val mps = collection.getAllMultiPolygonFeatures[T]
    ps.map(p => p.mapGeom(MultiPolygon(_))) ++ mps
  }

  def url(name: String): URL = {
    getClass.getResource(s"/$name")
  }

  def uri(name: String): URI = {
    getClass.getResource(s"/$name").toURI
  }

  def path(name: String): String = {
    getClass.getResource(s"/$name").getFile
  }
}