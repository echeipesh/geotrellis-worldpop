package geotrellis.jobs.admin

import cats.Monoid
import geotrellis.raster.{Raster, Tile, isData}
import geotrellis.raster.histogram.StreamingHistogram
import geotrellis.raster.summary.GridVisitor

case class PopulationSummary(population: Double, distribution: StreamingHistogram)

object PopulationSummary {
  implicit val populationSummaryMonoid: Monoid[PopulationSummary] = new Monoid[PopulationSummary] {
    override def empty: PopulationSummary = PopulationSummary(0, StreamingHistogram(64))

    override def combine(x: PopulationSummary, y: PopulationSummary): PopulationSummary = {
      PopulationSummary(x.population + y.population, x.distribution.merge(y.distribution))
    }
  }

  class Visitor extends GridVisitor[Raster[Tile], PopulationSummary] {
    private var sum: Double = 0
    private val hist: StreamingHistogram = StreamingHistogram(64)

    def result = PopulationSummary(sum, hist)

    def visit(raster: Raster[Tile], col: Int, row: Int): Unit = {
      val v = raster.tile.getDouble(col, row)
      if (isData(v)) {
        hist.countItem(v, count = 1)
        sum += v
      }
      hist.statistics()
    }
  }
}
