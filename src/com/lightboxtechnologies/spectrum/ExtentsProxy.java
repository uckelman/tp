package com.lightboxtechnologies.spectrum;

import java.io.InputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;

class ExtentsProxy implements StreamProxy {
  public InputStream open(FileSystem fs, Path mf, List<Map<String,Object>> extents) throws IOException {
    final MapFile.Reader r = new MapFile.Reader(mf, fs.getConf());
    return new ExtentsInputStream(r, extents);
  }
}
