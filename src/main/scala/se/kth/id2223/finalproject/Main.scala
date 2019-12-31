package se.kth.id2223.finalproject

import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{FileVisitResult, Files, Path, Paths, SimpleFileVisitor}

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

  val ALL: Array[RoofMaterial] = Array(Incomplete, ConcreteCement, IrregularMetal, HealthyMetal, Other)

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
    val (input, output) = args match {
      case Array(input, output) => (input, output)
      case Array(input) => (input, "output")
      case Array() => ("stac-data/stac", "output")
    }

    RoofMaterial.ALL.foreach { rm =>
      Files.createDirectories(Paths.get(output, rm.toString))
    }

    Files.walkFileTree(Paths.get(input), new SimpleFileVisitor[Path] {
      override def preVisitDirectory(dir: Path, attrs: BasicFileAttributes): FileVisitResult = {
        val pathsInDir = Files.list(dir).iterator().asScala.toList
        val tiffFile = pathsInDir.collectFirst { case path if path.toString.endsWith(".tif") => path }
        val trainGeoJsonFile = pathsInDir.collectFirst {
          case path if path.getFileName.toString.startsWith("train-") && path.toString.endsWith(".geojson") => path
        }

        var found = false

        tiffFile.zip(trainGeoJsonFile).foreach { case (tiff, geoJson) =>
          processGeoTiff(tiff.toString, geoJson.toString, output)
          found = true
        }

        if (found) FileVisitResult.SKIP_SUBTREE else FileVisitResult.CONTINUE
      }
    })
  }

  def processGeoTiff(imagePath: String, geoJsonPath: String, outputDirectory: String): Unit = {
    val dirName = Paths.get(imagePath).getParent.toString

    // streaming GeoTiffs are not thread-safe, so each thread gets its own copy
    val image = ThreadLocal.withInitial[MultibandGeoTiff](() => GeoTiffReader.readMultiband(imagePath, streaming = true))
    val json = GeoJson.fromFile[JsonFeatureCollection](geoJsonPath)
    val features = json.getAllFeatures[Feature[Geometry, Properties]]

    println(f"processing $dirName")
    val pb = new ProgressBar(f"extracting features", features.size)

    features.par.foreach { feature =>
      saveFeatureAsSeparatePng(feature, image.get(), outputDirectory)
      pb.step()
    }

    pb.close()
  }

  def saveFeatureAsSeparatePng(feature: Feature[Geometry, Properties], image: MultibandGeoTiff, outputDirectory: String): Unit = {
    val Properties(_, material, id) = feature.data
    val extentInFeatureCoords: Extent = feature.geom.getEnvelopeInternal
    // the coordinate system of the features is geographical latitude and longitude
    val extentInImageCoords = extentInFeatureCoords.reproject(LatLng, image.crs)
    val croppedImage = image.crop(extentInImageCoords)

    croppedImage.tile.renderPng().write(s"$outputDirectory/$material/$id.png")
  }
}
