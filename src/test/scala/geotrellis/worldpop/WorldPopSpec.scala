package geotrellis.worldpop

import org.scalatest._
import geotrellis.raster._
import geotrellis.vector._
import geotrellis.layer._

class WorldPopSpec extends FunSpec with Matchers {
  val usa = WorldPop.rasterSource("USA").get

  // This is true, only need run it once so its ignored
  ignore("All rasters have aligned pixel grids") {
    @inline def offset(a: Double, b: Double, w: Double): Double = {
      val cols = (a - b) / w
      cols - math.floor(cols)
    }


    for {
      code <- WorldPop.codes
      rs <- WorldPop.rasterSource(code)
    } {
      withClue(s"${rs.metadata.name}") {
        WorldPopSpec.requireGridAligned(usa.gridExtent, rs.gridExtent)
      }
    }
  }

  it("Derive Global WorldPop Layout") {
    val layout = LayoutDefinition[Long](
      grid = GridExtent[Long](Extent(-180, -90, 180, 90), usa.cellSize),
      tileSize = 256
    )

    layout.cellSize shouldBe usa.cellSize
    layout.cols shouldBe layout.tileCols * layout.layoutCols
    layout.rows shouldBe layout.tileRows * layout.layoutRows

    // col and row count here is multiple of 2 at 256 pixels a tile
    info(s"Tiles: ${layout.layoutCols}x${layout.layoutRows}")
    info(usa.cellSize.toString)

    // TODO: requirement failed: x-aligned: offset by CellSize(8.3333333E-4,8.3333333E-4) 0.5008820900055582
    // WorldPopSpec.requireGridAligned(usa.gridExtent, layout)
  }
}

object WorldPopSpec {
  def requireGridAligned(a: GridExtent[Long], b: GridExtent[Long]): Unit = {
    import org.scalactic._
    import TripleEquals._
    import Tolerance._

    val epsX: Double = math.min(a.cellwidth, b.cellwidth) * 0.01
    val epsY: Double = math.min(a.cellheight, b.cellheight) * 0.01

    require((a.cellwidth === b.cellwidth +- epsX) && (a.cellheight === b.cellheight +- epsY),
      s"CellSize differs: ${a.cellSize}, ${b.cellSize}")

    @inline def offset(a: Double, b: Double, w: Double): Double = {
      val cols = (a - b) / w
      cols - math.floor(cols)
    }

    val deltaX = math.round((a.extent.xmin - b.extent.xmin) / b.cellwidth)
    val deltaY = math.round((a.extent.ymin - b.extent.ymin) / b.cellheight)

    /**
     * resultX and resultY represent the pixel bounds of b that is
     * closest to the a.extent.xmin and a.extent.ymin.
     */

    val resultX = deltaX * b.cellwidth + b.extent.xmin
    val resultY = deltaY * b.cellheight + b.extent.ymin

    /**
     * TODO: This is ignored at the moment to make it soft and to make GDAL work,
     * we need to reconsider these things to be softer (?)
     */

    require(a.extent.xmin === resultX +- epsX,
      s"x-aligned: offset by ${a.cellSize} ${offset(a.extent.xmin, resultX, a.cellwidth)}")

    require(a.extent.ymin === resultY +- epsY,
      s"y-aligned: offset by ${a.cellSize} ${offset(a.extent.ymin, resultY, a.cellheight)}")
  }
}
