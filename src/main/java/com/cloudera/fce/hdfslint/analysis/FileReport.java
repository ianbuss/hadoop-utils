package com.cloudera.fce.hdfslint.analysis;

import com.cloudera.fce.hdfslint.analysis.advisories.Advisory;
import org.apache.solr.common.SolrInputDocument;

import java.util.ArrayList;
import java.util.List;

public class FileReport {

  public final FileType type;
  public final CompressionType compressionType;
  public final long numBlocks;
  public final long fileSize;
  public final String fileName;
  public final String description;
  public final String scanDate;
  private List<Advisory> advisories;

  public FileReport(FileType type, long numBlocks, long fileSize, CompressionType compressionType,
                    String fileName, String scanDate) {
    this(type, numBlocks, fileSize, compressionType, fileName, scanDate, "");
  }

  public FileReport(FileType type, long numBlocks, long fileSize, CompressionType compressionType,
                    String fileName, String scanDate, String description) {
    this.type = type;
    this.numBlocks = numBlocks;
    this.fileSize = fileSize;
    this.compressionType = compressionType;
    this.fileName = fileName;
    this.advisories = new ArrayList<>();
    this.scanDate = scanDate;
    this.description = description;
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

  public SolrInputDocument toSolrDoc() {
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("id", fileName + "#" + scanDate);
    doc.addField("scanDate", scanDate);
    doc.addField("fileName", fileName);
    doc.addField("fileSize", fileSize);
    doc.addField("fileType", type.name());
    doc.addField("numBlocks", numBlocks);
    doc.addField("compressionType", compressionType.name());
    List<String> advisoryStrings = new ArrayList<>();
    for (Advisory advisory : advisories) advisoryStrings.add(advisory.name());
    doc.addField("advisory", advisoryStrings);
    doc.addField("description", description);

    return doc;
  }

}
