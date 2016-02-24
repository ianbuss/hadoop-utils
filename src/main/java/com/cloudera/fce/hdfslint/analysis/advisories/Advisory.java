package com.cloudera.fce.hdfslint.analysis.advisories;

public enum Advisory {

  FILE_SIZE("Files should not be much smaller than an HDFS block", new FileSizeCheck()),
  PARQ_SPLIT("Parquet files should not be split across HDFS blocks", new ParquetFileSplitCheck()),
  UNCOMPRESSED_TEXT("Compress large text files", new UncompressedTextCheck());

  public final String description;
  public final AdvisoryCheck check;

  Advisory(String description, AdvisoryCheck check) {
    this.description = description;
    this.check = check;
  }

  public String toJson() {
    return "\"" + this.name() + "\"";
  }

}
