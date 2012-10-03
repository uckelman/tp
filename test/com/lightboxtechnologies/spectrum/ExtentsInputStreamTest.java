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
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import org.junit.Test;
import org.junit.runner.RunWith;
import static org.junit.Assert.*;

import org.hamcrest.Description;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.api.Action;
import org.jmock.api.Invocation;
import org.jmock.integration.junit4.JMock;
import org.jmock.integration.junit4.JUnit4Mockery;
import org.jmock.lib.legacy.ClassImposteriser;

@RunWith(JMock.class)
public class ExtentsInputStreamTest {

  protected final Mockery context = new JUnit4Mockery() {
    {
      setImposteriser(ClassImposteriser.INSTANCE);
    }
  };

  protected static class FillBytesWritable implements Action {
    protected final byte[] src;

    public FillBytesWritable(byte[] src) {
      this.src = src;
    }

    public void describeTo(Description desc) {
      desc.appendText("fills a BytesWritable with bytes ")
          .appendText(src.toString());
    }

    public Object invoke(Invocation inv) throws Throwable {
      ((BytesWritable)inv.getParameter(1)).set(src, 0, src.length);
      return null;
    }
  }

  protected static Action fillBytesWritable(byte[] src) {
    return new FillBytesWritable(src);
  }

  protected static class ReturnNewLongWritable implements Action {
    protected final long val;

    public ReturnNewLongWritable(long val) {
      this.val = val;
    }

    public void describeTo(Description desc) {
      desc.appendText("creates a new LongWritable with value ")
          .appendValue(val);
    }

    public Object invoke(Invocation inv) throws Throwable {
      return new LongWritable(val);
    }
  }

  protected static Action returnNewLongWritable(long val) {
    return new ReturnNewLongWritable(val);
  }

  @Test
  public void readArrayNoExtentsTest() throws IOException {
    final List<Map<String,Object>> extents = Collections.emptyList();
    
    final MapFile.Reader r = context.mock(MapFile.Reader.class);
 
    final InputStream eis = new ExtentsInputStream(r, extents);

    final byte[] buf = { 17 };

    assertEquals(-1, eis.read(buf, 0, buf.length));
    assertEquals(17, buf[0]);
  }

  @Test
  public void readByteNoExtentsTest() throws IOException {
    final List<Map<String,Object>> extents = Collections.emptyList();
    
    final MapFile.Reader r = context.mock(MapFile.Reader.class);

    final InputStream eis = new ExtentsInputStream(r, extents);

    assertEquals(-1, eis.read());
  }

  @Test
  public void readArrayOneExtentTest() throws IOException {
    final Map<String,Object> extent = new HashMap<String,Object>();
    extent.put("addr", Long.valueOf(42));
    extent.put("len", Long.valueOf(10));
    
    final List<Map<String,Object>> extents = Collections.singletonList(extent);
 
    final byte[] src = new byte[1024]; 
    for (int i = 0; i < src.length; ++i) {
      src[i] = (byte) (i % 256);
    }

    final byte[] expected = new byte[10];
    System.arraycopy(src, 42, expected, 0, 10);

    final MapFile.Reader r = context.mock(MapFile.Reader.class);
    context.checking(new Expectations() {
      {
        oneOf(r).getClosest(
          with(any(WritableComparable.class)),
          with(any(Writable.class)),
          with(any(boolean.class))
        );
        will(doAll(
          fillBytesWritable(src),
          returnNewLongWritable(0))
        );
      }
    });

    final InputStream eis = new ExtentsInputStream(r, extents);

    final byte[] actual = new byte[expected.length];
    assertEquals(10, eis.read(actual, 0, actual.length));
    assertArrayEquals(expected, actual);
  }

  @Test
  public void readIntOneExtentTest() throws IOException {
    final Map<String,Object> extent = new HashMap<String,Object>();
    extent.put("addr", Long.valueOf(42));
    extent.put("len", Long.valueOf(10));
    
    final List<Map<String,Object>> extents = Collections.singletonList(extent);

    final byte[] src = new byte[1024]; 
    for (int i = 0; i < src.length; ++i) {
      src[i] = (byte) (i % 256);
    }

    final MapFile.Reader r = context.mock(MapFile.Reader.class);
    context.checking(new Expectations() {
      {
        oneOf(r).getClosest(
          with(any(WritableComparable.class)),
          with(any(Writable.class)),
          with(any(boolean.class))
        );
        will(doAll(
          fillBytesWritable(src),
          returnNewLongWritable(0))
        );
      }
    });

    final InputStream eis = new ExtentsInputStream(r, extents);

    assertEquals(src[42], eis.read());
  }

  @Test
  public void readArrayMultipleExtentsTest() throws IOException {
    final int n = 10;

    final byte[] src = new byte[2*n];
    for (int i = 0; i < src.length; ++i) {
      src[i] = (byte) (i % 256);
    }

    final List<Map<String,Object>> extents =
      new ArrayList<Map<String,Object>>();

    for (int i = 0, off = 0; i <= n; off += i, ++i) {
      final Map<String,Object> extent = new HashMap<String,Object>();
      extent.put("addr", Long.valueOf(i));
      extent.put("len", Long.valueOf(i));
      extents.add(extent);
    }      

    final MapFile.Reader r = context.mock(MapFile.Reader.class);
    context.checking(new Expectations() {
      {
        exactly(extents.size()).of(r).getClosest(
          with(any(WritableComparable.class)),
          with(any(Writable.class)),
          with(any(boolean.class))
        );
        will(doAll(
          fillBytesWritable(src),
          returnNewLongWritable(0))
        );
      }
    });

    final InputStream eis = new ExtentsInputStream(r, extents);

    for (int i = 0; i < extents.size(); ++i) {
      final int rlen = ((Number) extents.get(i).get("len")).intValue();
      
      final byte[] actual = new byte[rlen];
      assertEquals(rlen, eis.read(actual, 0, rlen));

      final int addr = ((Number) extents.get(i).get("addr")).intValue();
      final byte[] expected = Arrays.copyOfRange(src, addr, addr + rlen);

      assertArrayEquals(expected, actual);
    }
  }
}
