package com.github.jjfiv.pscores;

import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.list.array.TIntArrayList;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
* @author jfoley
*/
public class PositionalScoreArray {
	// Sparse representation of a feature vector over positions.
  public TIntArrayList positions;
  public TDoubleArrayList scores;

  public PositionalScoreArray() {
    positions = new TIntArrayList();
    scores = new TDoubleArrayList();
  }

  public void reserve(int numPositions) {
    positions.ensureCapacity(numPositions);
    scores.ensureCapacity(numPositions);
  }

  /** Builder */
  public void clear() {
    positions.clear();
    scores.clear();
  }

  public boolean isEmpty() {
    return positions.isEmpty();
  }

  /** I/O */
  public void read(DataInput data) throws IOException {
    clear();
    int numPositions = data.readInt();
    this.reserve(numPositions);

    int offset = 0;
    // delta decode positions
    for (int i = 0; i < numPositions; i++) {
      offset += data.readInt();
      positions.add(offset);
    }

    //direct decode scores
    for (int i = 0; i < numPositions; i++) {
      scores.add(data.readDouble());
    }
  }

  /** I/O */
  public void write(DataOutput data) throws IOException {
    assert(scores.size() == positions.size());
    // difference encode positions
    data.writeInt(positions.size());
    int offset = 0;
    for (int i = 0; i < positions.size(); i++) {
      int dpos = positions.getQuick(i)-offset;
      data.writeInt(dpos);
      offset += dpos;
      assert(offset == positions.getQuick(i));
    }

    //direct encode scores; same size as positions
    for (int i = 0; i < scores.size(); i++) {
      data.writeDouble(scores.getQuick(i));
    }
  }

  /** Builder */
  public void addPosition(int position) {
    positions.add(position);
  }

  /** Builder */
  public void addScore(double score) {
    scores.add(score);
  }
}
