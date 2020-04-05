package geotrellis.worldpop

import geotrellis.layer._
import geotrellis.raster._
import geotrellis.raster.geotiff.GeoTiffRasterSource
import geotrellis.raster.resample.NearestNeighbor
import geotrellis.spark.partition.SpatialPartitioner
import geotrellis.spark._
import geotrellis.vector.Extent
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{HashPartitioner, Partitioner, SparkContext}

import scala.collection.concurrent.TrieMap
import scala.reflect.ClassTag

object WorldPop {
  // ensure we have only one RasterSouce per country per JVM to avoid re-reading metadata
  private val rasterSourceCache: TrieMap[String, RasterSource] = TrieMap.empty

  def rasterSource(code: String): Option[RasterSource] = {
    def getSource(cc: String) = rasterSourceCache.getOrElseUpdate(cc, {
      val base = "s3://azavea-worldpop/Population/Global_2000_2020/2020"
      val url = s"${base}/$cc/${cc.toLowerCase}_ppp_2020.tif" // not UN adj.
      GeoTiffRasterSource(url)
    })

    val cleanCode = code.toUpperCase()
    if (WorldPop.codes.contains(cleanCode)) {
      Some(getSource(cleanCode))
    } else None
  }

  def layerRdd(
    sources: Array[RasterSource],
    layout: LayoutDefinition,
    resampleMethod: ResampleMethod = NearestNeighbor,
    partitioner: Option[Partitioner] = None
  )(implicit spark: SparkSession): TileLayerRDD[SpatialKey] = {
    val keyExtractor = KeyExtractor.spatialKeyExtractor
    val summary = RasterSummary.fromSeq(sources, keyExtractor.getMetadata)
    val layerMetadata = summary.toTileLayerMetadata(layout, keyExtractor.getKey)
    val partitionCount = summary.estimatePartitionsNumber
    val part = partitioner.getOrElse(new HashPartitioner(partitionCount))

    val sharedSources = spark.sparkContext.broadcast(
      sources.map(_.tileToLayout(layout, sk => sk)))

    val rasterRegionRDD: RDD[(SpatialKey, (SpatialKey, Int))] = spark.sparkContext
      .parallelize( sources.indices, sources.length)
      .flatMap { indx =>
        sharedSources.value(indx).keys.map( key => key -> (key, indx))
      }
      .partitionBy(part)

    val tiledRDD: RDD[(SpatialKey, Tile)] =
      rasterRegionRDD
        .groupByKey(part)
        .mapValues { iter => {
          for {
            (key, index) <- iter.toSeq
            tile <- sharedSources.value(index).read(key)
          } yield tile.band(0)
        }.reduce (_ merge _) }

    ContextRDD(tiledRDD, layerMetadata)
  }


  /** Map from WorldPop country code to country name */
  val countryName: Map[String, String] = Map(
    "AFG" -> "Afghanistan",
    "ALB" -> "Albania",
    "DZA" -> "Algeria",
    "AND" -> "Andorra",
    "AGO" -> "Angola",
    "ATG" -> "Antigua and Barbuda",
    "ARG" -> "Argentina",
    "ARM" -> "Armenia",
    "AUS" -> "Australia",
    "AUT" -> "Austria",
    "AZE" -> "Azerbaijan",
    "BHS" -> "The Bahamas",
    "BHR" -> "Bahrain",
    "BGD" -> "Bangladesh",
    "BRB" -> "Barbados",
    "BLR" -> "Belarus",
    "BEL" -> "Belgium",
    "BLZ" -> "Belize",
    "BEN" -> "Benin",
    "BMU" -> "Bermuda",
    "BTN" -> "Bhutan",
    "BOL" -> "Bolivia",
    "BIH" -> "Bosnia and Herzegovina",
    "BWA" -> "Botswana",
    "BRA" -> "Brazil",
    "BRN" -> "Brunei",
    "BGR" -> "Bulgaria",
    "BFA" -> "Burkina Faso",
    "BDI" -> "Burundi",
    "KHM" -> "Cambodia",
    "CMR" -> "Cameroon",
    "CAN" -> "Canada",
    "CPV" -> "Cape Verde",
    "CAF" -> "Central African Republic",
    "TCD" -> "Chad",
    "CHL" -> "Chile",
    "CHN" -> "China",
    "COL" -> "Colombia",
    "COM" -> "Comoros",
    "COD" -> "Democratic Republic of the Congo",
    "COG" -> "Republic Of Congo",
    "CRI" -> "Costa Rica",
    "CIV" -> "Ivory Coast",
    "HRV" -> "Croatia",
    "CUB" -> "Cuba",
    "CYP" -> "Cyprus",
    "CZE" -> "Czech Republic",
    "DNK" -> "Denmark",
    "DJI" -> "Djibouti",
    "DMA" -> "Dominica",
    "DOM" -> "Dominican Republic",
    "ECU" -> "Ecuador",
    "EGY" -> "Egypt",
    "SLV" -> "El Salvador",
    "GNQ" -> "Equatorial Guinea",
    "ERI" -> "Eritrea",
    "EST" -> "Estonia",
    "ETH" -> "Ethiopia",
    "FJI" -> "Fiji",
    "FIN" -> "Finland",
    "FRA" -> "France",
    "GAB" -> "Gabon",
    "GMB" -> "Gambia",
    "GEO" -> "Georgia",
    "DEU" -> "Germany",
    "GHA" -> "Ghana",
    "GRC" -> "Greece",
    "GRL" -> "Greenland",
    "GRD" -> "Grenada",
    "GUM" -> "Guam",
    "GTM" -> "Guatemala",
    "GIN" -> "Guinea",
    "GNB" -> "Guinea Bissau",
    "GUY" -> "Guyana",
    "HTI" -> "Haiti",
    "HND" -> "Honduras",
    "HUN" -> "Hungary",
    "ISL" -> "Iceland",
    "IND" -> "India",
    "IDN" -> "Indonesia",
    "IRN" -> "Iran",
    "IRQ" -> "Iraq",
    "IRL" -> "Ireland",
    "ISR" -> "Israel",
    "ITA" -> "Italy",
    "JAM" -> "Jamaica",
    "JPN" -> "Japan",
    "JEY" -> "Jersey",
    "JOR" -> "Jordan",
    "KAZ" -> "Kazakhstan",
    "KEN" -> "Kenya",
    "KOS" -> "Kosovo",
    "KIR" -> "Kiribati",
    "PRK" -> "North Korea",
    "KOR" -> "South Korea",
    "KWT" -> "Kuwait",
    "KGZ" -> "Kyrgyzstan",
    "LAO" -> "Laos",
    "LVA" -> "Latvia",
    "LBN" -> "Lebanon",
    "LSO" -> "Lesotho",
    "LBR" -> "Liberia",
    "LBY" -> "Libya",
    "LIE" -> "Liechtenstein",
    "LTU" -> "Lithuania",
    "LUX" -> "Luxembourg",
    "MKD" -> "Macedonia",
    "MDG" -> "Madagascar",
    "MWI" -> "Malawi",
    "MYS" -> "Malaysia",
    "MDV" -> "Maldives",
    "MLI" -> "Mali",
    "MLT" -> "Malta",
    "MRT" -> "Mauritania",
    "MUS" -> "Mauritius",
    "MEX" -> "Mexico",
    "FSM" -> "Federated States of Micronesia",
    "MDA" -> "Moldova",
    "MCO" -> "Monaco",
    "MNG" -> "Mongolia",
    "MNE" -> "Montenegro",
    "MAR" -> "Morocco",
    "MOZ" -> "Mozambique",
    "MMR" -> "Myanmar",
    "NAM" -> "Namibia",
    "NRU" -> "Nauru",
    "NPL" -> "Nepal",
    "NLD" -> "Netherlands",
    "NZL" -> "New Zealand",
    "NIC" -> "Nicaragua",
    "NER" -> "Niger",
    "NGA" -> "Nigeria",
    "NIU" -> "Niue",
    "MNP" -> "Northern Mariana Islands",
    "NOR" -> "Norway",
    "OMN" -> "Oman",
    "PAK" -> "Pakistan",
    "PLW" -> "Palau",
    "PSE" -> "Palestine",
    "PAN" -> "Panama",
    "PNG" -> "Papua New Guinea",
    "PRY" -> "Paraguay",
    "PER" -> "Peru",
    "PHL" -> "Philippines",
    "POL" -> "Poland",
    "PRT" -> "Portugal",
    "PRI" -> "Puerto Rico",
    "QAT" -> "Qatar",
    "ROU" -> "Romania",
    "RWA" -> "Rwanda",
    "RUS" -> "Russia",
    "SHN" -> "Saint Helena",
    "KNA" -> "Saint Kitts and Nevis",
    "LCA" -> "Saint Lucia",
    "VCT" -> "Saint Vincent and the Grenadines",
    "WSM" -> "Samoa",
    "SMR" -> "San Marino",
    "STP" -> "Sao Tome and Principe",
    "SAU" -> "Saudi Arabia",
    "SEN" -> "Senegal",
    "SRB" -> "Republic of Serbia",
    "SYC" -> "Seychelles",
    "SLE" -> "Sierra Leone",
    "SGP" -> "Singapore",
    "SVK" -> "Slovakia",
    "SVN" -> "Slovenia",
    "SLB" -> "Solomon Islands",
    "SOM" -> "Somalia",
//    "SOL" -> "Somaliland", //TODO: find Somaliiland
    "ZAF" -> "South Africa",
    "SSD" -> "South Sudan",
    "ESP" -> "Spain",
    "LKA" -> "Sri Lanka",
    "SDN" -> "Sudan",
    "SUR" -> "Suriname",
    "SWZ" -> "Swaziland",
    "SWE" -> "Sweden",
    "CHE" -> "Switzerland",
    "SYR" -> "Syria",
    "TWN" -> "Taiwan",
    "TJK" -> "Tajikistan",
    "TZA" -> "United Republic of Tanzania",
    "THA" -> "Thailand",
    "TLS" -> "East Timor",
    "TGO" -> "Togo",
    "TON" -> "Tonga",
    "TTO" -> "Trinidad and Tobago",
    "TUN" -> "Tunisia",
    "TUR" -> "Turkey",
    "TKM" -> "Turkmenistan",
    "TCA" -> "Turks and Caicos Islands",
    "UGA" -> "Uganda",
    "UKR" -> "Ukraine",
    "ARE" -> "United Arab Emirates",
    "GBR" -> "United Kingdom",
    "USA" -> "United States Of America",
    "URY" -> "Uruguay",
    "UZB" -> "Uzbekistan",
    "VUT" -> "Vanuatu",
    "VEN" -> "Venezuela",
    "VNM" -> "Vietnam",
    "ESH" -> "Western Sahara",
    "YEM" -> "Yemen",
    "ZMB" -> "Zambia",
    "ZWE" -> "Zimbabwe"
  )

  val codes: Set[String] = countryName.keys.toSet
}
