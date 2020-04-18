package geotrellis.jobs.render

import geotrellis.raster.render.{ColorRamp, ColorMap, ColorRamps}

object WorldPopColorMap {
  val MAX_WORLD_POP_PIXEL_VALUE = 27803

  val orange9 = ColorRamp(
    0x0000000,
    0xfee6ceff,
    0xfdd0a2ff,
    0xfdae6bff,
    0xfd8d3cff,
    0xf16913ff,
    0xd94801ff,
    0xa63603ff,
    0x7f2704ff
  )

  val blue9 = ColorRamp(
    0x000000,
    0xdfecf8,
    0xc8ddf0,
    0xa3cce3,
    0x73b3d8,
    0x4a97c9,
    0x2879b9,
    0x0d58a1,
    0x08306b)

  val globalBreaks = Array(1, 2, 5, 10, 20, 50, 100, 200, 1000)
  val twofoldBreaks = Array(1,2,4,8,16,32,64,128,256,512,1025,2048)

  val BlueRamp = ColorRamp(0xFFF7FBFF,0xECE7F2FF,0xD0D1E6FF,0xA6BDDBFF,0x74A9CFFF,0x3690C0FF,0x0570B0FF,0x034E7BFF)

  def firstBreakTransparent(cr: ColorRamp, breaks: Array[Int]): ColorMap = {
    ColorRamp(0 +: cr.stops(breaks.length - 1).colors).toColorMap(breaks)
  }

  def globalBlueToRed = ColorRamps.BlueToRed.toColorMap(globalBreaks)

  val blueColorMap = firstBreakTransparent(BlueRamp, globalBreaks)
}