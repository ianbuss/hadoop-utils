package com.cloudera.fce.hdfslint.spark

import com.typesafe.scalalogging.slf4j.LazyLogging

case class ImageRecord(path: String,
                       rep: Int,
                       modTime: String,
                       accessTime: String,
                       bs: Long,
                       blocks: Int,
                       size: Long,
                       perms: String,
                       user: String,
                       group: String,
                       deleted: Boolean = false) {
  def ==(other: ImageRecord): Boolean = {
    path == other.path && size == other.size && modTime == other.modTime
  }
}

object ImageRecord extends LazyLogging {
  def parseFromString(s: String, delimiter: String = "\t"): ImageRecord = {
    val fields = s.split(delimiter, 10)
    if (fields.length != 10) {
      logger.error(s"Could not parse ${s} as an ImageRecord")
      throw new IllegalArgumentException
    }
    else ImageRecord(
      fields(0),
      fields(1).toInt,
      fields(2),
      fields(3),
      fields(4).toLong,
      fields(5).toInt,
      fields(6).toLong,
      fields(7),
      fields(8),
      fields(9)
    )
  }

  def markDeleted(r: ImageRecord): ImageRecord = {
    ImageRecord(
      r.path,
      r.rep,
      r.modTime,
      r.accessTime,
      r.bs,
      r.blocks,
      r.size,
      r.perms,
      r.user,
      r.group,
      true
    )
  }
}
