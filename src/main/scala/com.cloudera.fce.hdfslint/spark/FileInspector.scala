package com.cloudera.fce.hdfslint.spark

import com.cloudera.fce.hdfslint.analysis.advisories.Advisory
import com.cloudera.fce.hdfslint.analysis.{CompressionType, FileType}
import com.cloudera.fce.hdfslint.detectors._
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.hadoop.fs.shell.PathData
import org.apache.solr.common.SolrInputDocument

import scala.collection.JavaConversions._

trait SolrRepresentation {
  def toSolr: SolrInputDocument
}

case class FileReport(fType: String,
                      numBlocks: Long,
                      fileSize: Long,
                      compressionType: String,
                      fileName: String,
                      scanID: String,
                      description: String,
                      advisories: List[String]) extends SolrRepresentation {
  override def toSolr: SolrInputDocument = {
    val doc = new SolrInputDocument
    doc.addField("id", s"${fileName}#${scanID}")
    doc.addField("scanID", scanID)
    doc.addField("fileName", fileName)
    doc.addField("fileSize", fileSize)
    doc.addField("fileType", fType)
    doc.addField("numBlocks", numBlocks)
    doc.addField("compressionType", compressionType)
    doc.addField("advisories", advisories.mkString(" "))
    doc.addField("description", description)
    doc
  }
}

case class DirectorySummary(path: String,
                            numFiles: Long,
                            compressionTypes: Set[String],
                            advisories: Set[String],
                            scanID: String) extends SolrRepresentation {
  def mergeSummary(other: DirectorySummary): DirectorySummary = {
    DirectorySummary(
      path,
      numFiles + other.numFiles,
      compressionTypes ++ other.compressionTypes,
      advisories ++ other.advisories,
      scanID
    )
  }

  override def toSolr: SolrInputDocument = {
    val doc = new SolrInputDocument
    doc.addField("id", s"${path}#${scanID}")

    doc
  }
}

class FileInspector extends LazyLogging {

  val headerReadLen = 20
  val headerBuf = new Array[Byte](headerReadLen)

  val detectors = List(
    new ParquetDetector,
    new AvroDetector,
    new SequenceFileDetector,
    new RCFileDetector,
    new ORCFileDetector,
    // put this one last
    new LinuxFileDetector
  )

  def inspectFile(file: PathData, scanID: String): Option[FileReport] = {
    def applyDetector(buf: Array[Byte], size: Int, detectors: List[Detector]): FileReport = detectors match {
      case d :: ds => if (d.detect(headerBuf, size)) {
        val fr = d.analyze(file, scanID)
        return FileReport(
          fr.`type`.name,
          fr.numBlocks,
          fr.fileSize,
          fr.compressionType.name,
          fr.fileName,
          scanID,
          fr.description,
          (for (a: Advisory <- fr.getAdvisories) yield a.name).toList
        )
      } else applyDetector(buf, size, ds)
      case _ => FileReport(
        FileType.OTHER.name,
        file.fs.getFileBlockLocations(file.stat, 0, file.stat.getLen).length,
        file.stat.getLen,
        CompressionType.UNKNOWN.name,
        file.path.toString,
        scanID,
        "",
        List()
      )
    }

    if (file.fs.exists(file.path) && file.fs.isFile(file.path)) {
      val is = file.fs.open(file.path)
      try {
        val read = is.read(headerBuf, 0, headerReadLen)
        Some(applyDetector(headerBuf, read, detectors))
      } catch {
        case e: Exception => {
          logger.error(s"Error reading ${file.path}: ${e.getMessage}")
          None
        }
      } finally {
        if (is != null) is.close()
      }
    } else {
      None
    }
  }

}
