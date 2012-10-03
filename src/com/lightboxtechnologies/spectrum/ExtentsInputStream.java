/*
Copyright 2011, Lightbox Technologies, Inc

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package com.lightboxtechnologies.spectrum;

import java.io.InputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapFile;

/**
 * An {@link InputStream} which reads sequentially from a list of extents.
 *
 * @author Joel Uckelman
 */
public class ExtentsInputStream extends InputStream {

  protected final MapFile.Reader r;
  protected LongWritable key = new LongWritable();
  protected final BytesWritable val = new BytesWritable(new byte[1024*1024]);

  protected final Iterator<Map<String,Object>> ext_iter;

  public ExtentsInputStream(MapFile.Reader mfreader, List<Map<String,Object>> extents) {
    r = mfreader;
    ext_iter = extents.iterator();
  }

  protected Map<String,?> cur_extent = null;
  protected long cur_pos;
  protected long cur_end;
 
  protected boolean prepareExtent() {
    // prepare next extent
    if (cur_extent == null || cur_pos == cur_end) {
      if (ext_iter.hasNext()) {
        cur_extent = ext_iter.next();

        final long length = ((Number) cur_extent.get("len")).longValue();

        cur_pos = ((Number) cur_extent.get("addr")).longValue();
        cur_end = cur_pos + length;
      }
      else {
        return false;
      }
    }

    return true;
  }

  @Override
  public int read(byte[] buf, int off, int len) throws IOException {
    if (!prepareExtent()) {
      return -1;
    }

    // find the block containing the current position
    key.set(cur_pos);
    key = (LongWritable) r.getClosest(key, val, true);
    if (key == null) {
      // EOF
      return -1;
    }

    // index into the retrieved byte block
    // NB: int cast is ok because difference isn't more than 2^20
    final int boff = (int)(cur_pos - key.get());
    final byte[] block = val.getBytes();

    // copy from the block to the buffer
    final int rlen = Math.min(len, block.length - boff);
    System.arraycopy(block, boff, buf, off, rlen);
    cur_pos += rlen;

    return rlen;
  }

  @Override
  public int read() throws IOException {
    final byte[] b = new byte[1];

    // keep trying until we read one byte or hit EOF
    int rlen;
    do {
      rlen = read(b, 0, 1);
    } while (rlen == 0);

    return rlen == -1 ? -1 : b[0];
  }
}
