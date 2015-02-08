package com.github.jjfiv.pscores.scoring;

import com.github.jjfiv.pscores.PositionalScoreArray;
import com.github.jjfiv.pscores.PositionalScoreIterator;
import org.lemurproject.galago.core.retrieval.iterator.DisjunctionIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;

import java.io.IOException;

/**
 * SynonymIterator implements an iterator where the occurrence of concepts is the maximum of either at the given frame within a video:
 * #pscores-syn(a b)
 *
 * @author jfoley
 */
public class SynonymIterator extends DisjunctionIterator implements PositionalScoreIterator {
  public final PositionalScoreArray output;
  public final PositionalScoreIterator[] children;
  public PositionalScoreArray[] arrays;

  public SynonymIterator(NodeParameters np, PositionalScoreIterator[] queryIterators) {
    super(queryIterators);
    this.children = queryIterators;
    this.output = new PositionalScoreArray();
    this.arrays = new PositionalScoreArray[children.length];
  }

  public void fetchArrays(ScoringContext c) {
    // load up these arrays...
    for (int i = 0; i < children.length; i++) {
      PositionalScoreIterator child = children[i];
      arrays[i] = child.data(c);
    }
  }

  public int length(ScoringContext c) {
    fetchArrays(c);
    int length = 0;
    for (PositionalScoreArray arr : arrays) {
      if (arr.positions.size() > length) {
        length = arr.positions.size();
      }
    }
    return length;
  }

  @Override
  public PositionalScoreArray data(ScoringContext c) {
    int length = length(c);
    output.clear();

    for(int pos=0; pos<length; pos++) {
      output.addPosition(pos);
      double max = 0.0;
      for (PositionalScoreArray array : arrays) {
        double cscore = 0.0;
        if (pos < array.scores.size()) {
          cscore = array.scores.getQuick(pos);
        }
        if(cscore > max) max = cscore;
      }
      output.addScore(max);
    }

    return output;
  }

  @Override
  public String getValueString(ScoringContext sc) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public AnnotatedNode getAnnotatedNode(ScoringContext sc) throws IOException {
    throw new UnsupportedOperationException();
  }
}
