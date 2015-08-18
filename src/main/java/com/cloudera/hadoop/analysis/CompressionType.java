package com.cloudera.hadoop.analysis;

import org.apache.hadoop.io.compress.*;

public enum CompressionType {

  NONE,
  SNAPPY,
  GZIP,
  ZLIB,
  LZO,
  BZ2,
  DEFLATE,
  UNKNOWN,
  LZ4;

  public static CompressionType fromHadoopCodec(CompressionCodec codec) {
    if (codec == null) return CompressionType.NONE;

    CompressionType compressionType = CompressionType.UNKNOWN;
    if (codec.getClass().equals(GzipCodec.class)) {
      compressionType = CompressionType.GZIP;
    } else if (codec.getClass().equals(DefaultCodec.class)) {
      compressionType = CompressionType.DEFLATE;
    } else if (codec.getClass().equals(SnappyCodec.class)) {
      compressionType = CompressionType.SNAPPY;
    } else if (codec.getClass().equals(Lz4Codec.class)) {
      compressionType = CompressionType.LZ4;
    } else if (codec.getClass().equals(BZip2Codec.class)) {
      compressionType = CompressionType.BZ2;
    }

    return compressionType;
  }

}
