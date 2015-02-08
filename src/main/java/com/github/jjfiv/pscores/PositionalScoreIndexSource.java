package com.github.jjfiv.pscores;

import org.lemurproject.galago.core.index.source.BTreeValueSource;
import org.lemurproject.galago.core.index.source.DataSource;
import org.lemurproject.galago.utility.btree.BTreeIterator;
import org.lemurproject.galago.utility.buffer.VByteInput;

import java.io.IOException;

/**
* @author jfoley
*/
public class PositionalScoreIndexSource extends BTreeValueSource implements DataSource<PositionalScoreArray> {
  public static final PositionalScoreArray EMPTY = new PositionalScoreArray();

  VByteInput data;
  boolean hasCurrent;
  long docId;

  PositionalScoreArray scoreArray;

  public PositionalScoreIndexSource(BTreeIterator it) throws IOException {
    super(it);
    initialize();
  }

  private void initialize() throws IOException {
    data = new VByteInput(btreeIter.getValueStream());
    hasCurrent = true;
    scoreArray = new PositionalScoreArray();
    readNextDocument();
  }

  private void readNextDocument() throws IOException {
    if(!hasCurrent) return;

    hasCurrent = data.readBoolean();
    if(!hasCurrent) return;

    docId = data.readLong();
    scoreArray.read(data);
  }

  @Override
  public PositionalScoreArray data(long id) {
    if(id == docId) {
      return scoreArray;
    }
    return EMPTY;
  }

  @Override
  public void reset() throws IOException {
    initialize();
  }

  @Override
  public boolean isDone() {
    return !hasCurrent;
  }

  @Override
  public boolean hasAllCandidates() {
    return false;
  }

  @Override
  public long totalEntries() {
    throw new UnsupportedOperationException();
  }

  @Override
  public long currentCandidate() {
    return docId;
  }

  @Override
  public void movePast(long id) throws IOException {
    syncTo(id+1);
  }

  @Override
  public void syncTo(long id) throws IOException {
    while(hasCurrent && docId < id) readNextDocument();
  }
}
