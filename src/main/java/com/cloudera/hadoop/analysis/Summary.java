package com.cloudera.hadoop.analysis;

import com.cloudera.hadoop.analysis.advisories.Advisory;

import java.util.HashMap;
import java.util.Map;

public class Summary {

  private String rootName;
  private long numFiles;
  private Map<FileType, Long> types = new HashMap<>();
  private Map<CompressionType, Long> compressionTypes = new HashMap<>();
  private Map<Advisory, Long> advisoryTypes = new HashMap<>();

  public Summary(String rootName) {
    this.rootName = rootName;
  }

  public void addReport(FileReport report) {
    numFiles++;
    if (types.containsKey(report.type)) {
      types.put(report.type, types.get(report.type) + 1);
    } else {
      types.put(report.type, 1l);
    }

    if (compressionTypes.containsKey(report.compressionType)) {
      compressionTypes.put(report.compressionType, compressionTypes.get(report.compressionType) + 1);
    } else {
      compressionTypes.put(report.compressionType, 1l);
    }

    for (Advisory advisory : report.getAdvisories()) {
      if (advisoryTypes.containsKey(advisory)) {
        advisoryTypes.put(advisory, advisoryTypes.get(advisory) + 1);
      } else {
        advisoryTypes.put(advisory, 1l);
      }
    }
  }

  @Override
  public String toString() {
    String summary = "Summary: \n";
    summary += "Root => " + rootName + "\n";
    summary += "Number of files => " + numFiles + "\n";
    summary += "File types => ";
    for (Map.Entry<FileType, Long> entry : types.entrySet()) {
      summary += entry.getKey() + ": " + entry.getValue() + ", ";
    }
    if (summary.endsWith(", ")) summary = summary.substring(0, summary.length() - 2);
    summary += "\nCompression types => ";
    for (Map.Entry<CompressionType, Long> entry : compressionTypes.entrySet()) {
      summary += entry.getKey() + ": " + entry.getValue() + ", ";
    }
    if (summary.endsWith(", ")) summary = summary.substring(0, summary.length() - 2);
    summary += "\nAdvisory types => ";
    for (Map.Entry<Advisory, Long> entry : advisoryTypes.entrySet()) {
      summary += entry.getKey() + ": " + entry.getValue() + ", ";
    }
    if (summary.endsWith(", ")) summary = summary.substring(0, summary.length() - 2);

    return summary;
  }

}
