package com.cloudera.fce.hdfslint.spark

import java.io._

import com.cloudera.fce.hdfslint.oiv.PBImageDelimitedTextWriter
import com.google.common.base.Charsets
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.shell.PathData
import org.apache.hadoop.fs.{FileSystem, LocatedFileStatus, Path, RemoteIterator}
import org.apache.hadoop.hdfs.tools.DFSAdmin
import org.apache.solr.client.solrj.impl.CloudSolrServer
import org.apache.solr.common.SolrInputDocument
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._

class SparkHDFSLint(val scanID: String) extends LazyLogging {

  val sc = SparkHDFSLint.initialiseSparkContext()
  val hdfs = SparkHDFSLint.initialiseHdfs(sc.hadoopConfiguration)
  val tstamp = s"${System.currentTimeMillis()}"
  val hdfsFileList = s"${SparkHDFSLint.IMAGE_LOC_HDFS}/$tstamp"

  val fullLoad = true
  val parts = 20

  def fetchImage(): Boolean = {
    val imageDir = Array(SparkHDFSLint.IMAGE_LOC)
    val dfsAdmin = new DFSAdmin(sc.hadoopConfiguration)
    try {
      dfsAdmin.fetchImage(imageDir, 0)
      true
    } catch {
      case e: Exception =>
        logger.error(s"Could not fetch HDFS image: ${e.getMessage}")
        false
    }
  }

  def processAndUploadImage(): Boolean = {
    logger.info(s"Reading downloaded image file: ${SparkHDFSLint.IMAGE_LOC}, writing to HDFS at: $hdfsFileList")
    try {
      val dos = hdfs.create(new Path(hdfsFileList))
      val ops = new PrintStream(dos, false, Charsets.UTF_8.displayName())
      val writer = new PBImageDelimitedTextWriter(ops, "\t", SparkHDFSLint.IMAGE_LDB_TMP)
      writer.visit(new RandomAccessFile(SparkHDFSLint.IMAGE_LOC, "r"))
      true
    } catch {
      case e: IOException =>
        logger.error(s"Could not process HDFS image: ${e.getMessage}")
        false
    }
  }

  def diffImages(): RDD[ImageRecord] = {
    def findLastImage(last: String, it: RemoteIterator[LocatedFileStatus]): String = {
      if (it.hasNext) {
        val f = it.next
        if (f.isFile && f.getPath.toUri.getPath != hdfsFileList && f.getPath.toUri.getPath > last) {
          findLastImage(f.getPath.toUri.getPath, it)
        } else {
          findLastImage(last, it)
        }
      } else {
        last
      }
    }
    val last = findLastImage("", hdfs.listFiles(new Path(SparkHDFSLint.IMAGE_LOC_HDFS), false))
    logger.info(s"Last image found at $last")

    val lastFiles = sc.textFile(last, parts).map(ImageRecord.parseFromString(_)).map(r => (r.path, r))
    logger.info(s"Last image RDD loaded from $last")
    val newFiles = sc.textFile(hdfsFileList, parts).map(ImageRecord.parseFromString(_)).map(r => (r.path, r))
    logger.info(s"New image RDD loaded from $hdfsFileList")
    val joined = newFiles.fullOuterJoin(lastFiles)
    joined.flatMap{ case (a, (r1, r2)) =>
      // New file
      if (r2.isEmpty) List(r1.get)
      // Deleted file
      else if (r1.isEmpty) List(ImageRecord.markDeleted(r2.get))
      // Changed?
      else {
        if (r1.get == r2.get) List()
        else List(r1.get)
      }
    }
  }

  def processFileList(files: RDD[ImageRecord]): RDD[FileReport] = {
    val scanIDb = sc.broadcast(scanID)
    files.mapPartitions(it => {
      val inspector = new FileInspector
      val conf = new Configuration
      it.flatMap(r => {
        if (!r.path.contains(".Trash")) inspector.inspectFile(new PathData(r.path, conf), scanIDb.value)
        else List()
      })
    })
  }

  def directorySummaries(reports: RDD[FileReport]): RDD[DirectorySummary] = {
    reports.flatMap(r => {
      def extractParentDirs(path: String, dirs: List[String]): List[String] = path match {
        case s if s.length > 1 =>
          val _s = s.replaceAll("hdfs://[^/]*","").replaceAll("/[^/]*/?$","/")
          extractParentDirs(_s, dirs :+ _s)
        case _ => dirs
      }
      val dirs = extractParentDirs(r.fileName, List())
      dirs.map(d => (d, DirectorySummary(
        d,
        1,
        Set(r.compressionType),
        r.advisories.toSet,
        r.scanID
      )))
    }).reduceByKey(_.mergeSummary(_)).map(_._2)
  }

  def pushToSolr(reports: RDD[_ <: SolrRepresentation],
                 zkEnsemble: String,
                 solrCollection: String,
                 batchSize: Int) = {
    val zkEnsembleb = sc.broadcast(zkEnsemble)
    val solrCollectionb = sc.broadcast(solrCollection)
    reports.foreachPartition(it => {
      def solrBatch(solrServer: CloudSolrServer, it: Iterable[_ <: SolrRepresentation], batch: List[SolrInputDocument]): Unit = it match {
        case r :: rs =>
          if (batch.length > batchSize) {
            solrServer.add(batch)
            solrBatch(solrServer, rs, List(r.toSolr))
          }
          solrBatch(solrServer, rs, batch :+ r.toSolr)
        case _ => if (batch.nonEmpty) solrServer.add(batch)
      }
      val solrServer = new CloudSolrServer(zkEnsembleb.value)
      solrServer.setDefaultCollection(solrCollectionb.value)
      solrBatch(solrServer, it.toIterable, List())
    })
  }

  def parseArgs(args: Array[String]): Unit = {

  }

  def run(): Unit = {
    // Fetch the image
    if (!fetchImage()) {
      logger.error("Failed to fetch image, cannot continue")
      sys.exit(-1)
    }

    // Process the image
    if (!processAndUploadImage()) {
      logger.error("Failed to process and upload image, cannot continue")
      sys.exit(-1)
    }

    val diff = diffImages()

    // Run the hdfslint process and insert file reports into Solr collection
    val files = processFileList(diff)
    pushToSolr(files, SparkHDFSLint.DEFAULT_ZK_ENSEMBLE, SparkHDFSLint.DEFAULT_COLLECTION, SparkHDFSLint.SOLR_BATCH_SIZE)

    // Collect summaries and aggregate
    val summaries = directorySummaries(files)
    pushToSolr(summaries, SparkHDFSLint.DEFAULT_ZK_ENSEMBLE, SparkHDFSLint.DEFAULT_COLLECTION, SparkHDFSLint.SOLR_BATCH_SIZE)
  }

}

object SparkHDFSLint {

  val IMAGE_LOC = "fsimage"
  val IMAGE_LDB_TMP = "fsimage.ldb.tmp"
  val IMAGE_LOC_HDFS = s"/user/${System.getProperty("user.name")}/images"
  val SOLR_BATCH_SIZE = 1000
  val DEFAULT_COLLECTION = "hdfslint"
  val DEFAULT_ZK_ENSEMBLE = "localhost:2181"

  def initialiseSparkContext(): SparkContext = {
    val conf = new SparkConf
    new SparkContext(conf)
  }

  def initialiseHdfs(conf: Configuration): FileSystem = {
    FileSystem.get(conf)
  }

  def main(args: Array[String]): Unit = {
    val hdfsLint = new SparkHDFSLint(args(0))
    hdfsLint.run()
  }

}