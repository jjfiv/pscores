package com.github.jjfiv.pscores;

import org.lemurproject.galago.core.btree.simple.DiskMapSortedBuilder;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.buffer.DiskSpillCompressedByteBuffer;
import org.lemurproject.galago.tupleflow.execution.Verified;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.utility.btree.IndexElement;
import org.lemurproject.galago.utility.buffer.VByteOutput;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
* @author jfoley
*/
@Verified
@InputClass(className="com.github.jjfiv.pscores.TermDocPositionScore", order={"+term", "+doc", "+position"})
public class PositionScoreIndexWriter implements TermDocPositionScore.TermDocPositionOrder.ShreddedProcessor {
  DiskMapSortedBuilder builder;
  PositionScoreInvertedList invertedList;
  PositionalScoreArray scoreArray;

  public PositionScoreIndexWriter(TupleFlowParameters tfp) throws IOException {
    this(tfp.getJSON().getString("filename"), tfp.getJSON());
  }

  public PositionScoreIndexWriter(String fileName, Parameters opts) throws IOException {
    opts.put("writerClass", getClass().getName());
    opts.put("readerClass", PositionScoreIndexReader.class.getName());
    opts.put("defaultOperator", PositionScoreIndexReader.getDefaultOperator());
    builder = new DiskMapSortedBuilder(fileName, opts);
    invertedList = null;
    scoreArray = new PositionalScoreArray();
  }

  @Override
  public void processTerm(int term) throws IOException {
    flush();
    invertedList = new PositionScoreInvertedList(term);
  }

  @Override
  public void processDoc(int doc) throws IOException {
    // put previous doc info...
    flushDocument();
    invertedList.putDocumentName(doc);
  }

  @Override
  public void processPosition(int position) throws IOException {
    scoreArray.addPosition(position);
  }

  @Override
  public void processTuple(double score) throws IOException {
    scoreArray.addScore(score);
  }

  @Override
  public void close() throws IOException {
    flush();
    builder.close();
  }

  private void flushDocument() throws IOException {
    if(scoreArray.isEmpty()) return;

    scoreArray.write(invertedList.data);
    scoreArray.clear();
  }

  private void flush() throws IOException {
    if(invertedList == null) return;

    // save last document
    flushDocument();
    // end of stream marker...
    invertedList.data.writeBoolean(false);

    // put this inverted list onto disk, and free the memory
    builder.putCustom(invertedList);

    invertedList = null;
  }

  public static class PositionScoreInvertedList implements IndexElement {
    private final byte[] term;
    VByteOutput data;
    private DiskSpillCompressedByteBuffer buffer;

    public PositionScoreInvertedList(int term) {
      this.term = Utility.fromInt(term);
      buffer = new DiskSpillCompressedByteBuffer();
      data = new VByteOutput(new DataOutputStream(buffer));
    }

    @Override
    public byte[] key() {
      return term;
    }

    @Override
    public long dataLength() {
      return buffer.length();
    }

    @Override
    public void write(OutputStream stream) throws IOException {
      //System.out.println("Writing buffer for "+ Utility.toInt(term)+": "+buffer.length());
      buffer.write(stream);
    }

    public void putDocumentName(int doc) throws IOException {
      data.writeBoolean(true);
      data.writeLong(doc);
    }
  }

}
