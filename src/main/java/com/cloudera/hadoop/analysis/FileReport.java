package com.cloudera.hadoop.analysis;

import com.cloudera.hadoop.analysis.advisories.Advisory;

import java.util.ArrayList;
import java.util.List;

public class FileReport {

  public final FileType type;
  public final CompressionType compressionType;
  public final long numBlocks;
  public final long fileSize;
  public final String fileName;
  private List<Advisory> advisories;

  public FileReport(FileType type, long numBlocks, long fileSize, CompressionType compressionType,
                    String fileName) {
    this.type = type;
    this.numBlocks = numBlocks;
    this.fileSize = fileSize;
    this.compressionType = compressionType;
    this.fileName = fileName;
    this.advisories = new ArrayList<>();
  }

  public void addAdvisory(Advisory advisory) {
    advisories.add(advisory);
  }

  public void addAdvisories(List<Advisory> advisories) {
    this.advisories.addAll(advisories);
  }

  public List<Advisory> getAdvisories() {
    return advisories;
  }

  @Override
  public String toString() {
    return "Name: " + fileName + ", Type: " + type + ", Size: " + fileSize +
      ", Num. blocks: " + numBlocks + ", Compression: " + compressionType + ", Advisories: " + advisories.toString();
  }

  public String toJson() {
    String json = "{\"name\": \"" + fileName + "\", \"type\": \"" + type + ", \"size\": " + fileSize +
      ", \"numBlocks\": " + numBlocks + ", \"compressionType\": \"" + compressionType +
      "\", \"advisories\": [";
    for (Advisory advisory : advisories) {
      json += advisory.toJson();
    }
    if (json.endsWith(",")) json = json.substring(0, json.length() - 1);
    return json + "]}";
  }

}
