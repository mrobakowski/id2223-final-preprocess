package se.kth.id2223.finalproject

import java.nio.file.{Files, Paths}

import geotrellis.proj4.LatLng
import geotrellis.raster.io.geotiff.MultibandGeoTiff
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import geotrellis.raster.Implicits._
import geotrellis.vector.io.json.GeoJsonSupport._
import geotrellis.vector.io.json.{GeoJson, JsonFeatureCollection}
import geotrellis.vector.{Extent, Feature, Geometry}
import io.circe.Decoder
import me.tongfei.progressbar.ProgressBar

import scala.collection.JavaConverters._

sealed trait RoofMaterial {
  override def toString: String = this match {
    case RoofMaterial.Incomplete => "incomplete"
    case RoofMaterial.ConcreteCement => "concrete_cement"
    case RoofMaterial.IrregularMetal => "irregular_metal"
    case RoofMaterial.HealthyMetal => "healthy_metal"
    case RoofMaterial.Other => "other"
  }
}

object RoofMaterial {

  case object Incomplete extends RoofMaterial

  case object ConcreteCement extends RoofMaterial

  case object IrregularMetal extends RoofMaterial

  case object HealthyMetal extends RoofMaterial

  case object Other extends RoofMaterial

  implicit val decoder: Decoder[RoofMaterial] = Decoder.decodeString.emap {
    case "incomplete" => Right(Incomplete)
    case "concrete_cement" => Right(ConcreteCement)
    case "irregular_metal" => Right(IrregularMetal)
    case "healthy_metal" => Right(HealthyMetal)
    case "other" => Right(Other)
    case _ => Left("unknown roof material")
  }
}

case class Properties(verified: Boolean, roofMaterial: RoofMaterial, id: String)

object Properties {
  implicit val decoder: Decoder[Properties] = Decoder.forProduct3("verified", "roof_material", "id")(Properties.apply)
}

object Main {
  def main(args: Array[String]): Unit = {
    val image = GeoTiffReader.readMultiband("stac-data/stac/colombia/borde_rural/borde_rural_ortho-cog.tif", streaming = true)
    val json = GeoJson.fromFile[JsonFeatureCollection]("stac-data/stac/colombia/borde_rural/train-borde_rural.geojson")
    val features = json.getAllFeatures[Feature[Geometry, Properties]]

    // Streaming GeoTiff is not thread-safe, so this has to be done on one thread, sadly...
    // TODO: maybe let's have every thread access a thread-local image???
    ProgressBar.wrap(features.asJava, "extracting images").forEach { feature =>
      extractFeatureFromImage(feature, image)
    }

    extractFeatureFromImage(features(0), image)
  }

  def extractFeatureFromImage(feature: Feature[Geometry, Properties], image: MultibandGeoTiff) = {
    val Properties(_, material, id) = feature.data
    val extentInFeatureCoords: Extent = feature.geom.getEnvelopeInternal
    // the coordinate system of the features is geographical latitude and longitude
    val extentInImageCoords = extentInFeatureCoords.reproject(LatLng, image.crs)
    val croppedImage = image.crop(extentInImageCoords)

    try {
      Files.createDirectories(Paths.get("output", material.toString))
    } catch {
      case _: Throwable => ()
    }

    croppedImage.tile.renderPng().write(s"output/$material/$id.png")
  }
}
