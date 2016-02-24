package com.cloudera.fce.hdfslint.analysis;

import com.cloudera.fce.hdfslint.analysis.advisories.Advisory;

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
    addKeyValue(types, report.type, 1l);
    addKeyValue(compressionTypes, report.compressionType, 1l);

    for (Advisory advisory : report.getAdvisories()) {
      addKeyValue(advisoryTypes, advisory, 1l);
    }
  }

  private <K> void addKeyValue(Map<K, Long> map, K key, long value) {
    if (map.containsKey(key)) {
      map.put(key, map.get(key) + value);
    } else {
      map.put(key, value);
    }
  }

  private <K> void addAllKeyValues(Map<K, Long> map, Map<K, Long> toAdd) {
    for (Map.Entry<K, Long> entry : toAdd.entrySet()) {
      if (map.containsKey(entry.getKey())) {
        map.put(entry.getKey(), map.get(entry.getKey()) + entry.getValue());
      } else {
        map.put(entry.getKey(), entry.getValue());
      }
    }
  }

  public void addSummary(Summary summary) {
    numFiles += summary.numFiles;
    addAllKeyValues(types, summary.types);
    addAllKeyValues(compressionTypes, summary.compressionTypes);
    addAllKeyValues(advisoryTypes, summary.advisoryTypes);
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

  public String toJson() {
    String summary = "{ \"root\": \"" + rootName + "\"";
    summary += ", \"numFiles\": " + numFiles;
    summary += ", \"fileTypes\": [";
    for (Map.Entry<FileType, Long> entry : types.entrySet()) {
      summary += "{\"" + entry.getKey() + "\": " + entry.getValue() + "}, ";
    }
    if (summary.endsWith(", ")) summary = summary.substring(0, summary.length() - 2);
    summary += "]";
    summary += ", \"compressionTypes\": [";
    for (Map.Entry<CompressionType, Long> entry : compressionTypes.entrySet()) {
      summary += "{\"" + entry.getKey() + "\": " + entry.getValue() + "}, ";
    }
    if (summary.endsWith(", ")) summary = summary.substring(0, summary.length() - 2);
    summary += "]";
    summary += ", \"advisoryTypes\": [";
    for (Map.Entry<Advisory, Long> entry : advisoryTypes.entrySet()) {
      summary += "{\"" + entry.getKey() + "\": " + entry.getValue() + "}, ";
    }
    if (summary.endsWith(", ")) summary = summary.substring(0, summary.length() - 2);
    summary += "] }";

    return summary;
  }

}
