package com.lightboxtechnologies.spectrum;

import java.io.InputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;

class ExtentsProxy implements StreamProxy {
  public InputStream open(FileSystem fs, FSDataInputStream di, FsEntry entry)
                                                           throws IOException {
    final List<Map<String,Object>> extents = 
      (List<Map<String,Object>>) entry.get("extents");
    return new ExtentsInputStream(di, extents);
  }
}
