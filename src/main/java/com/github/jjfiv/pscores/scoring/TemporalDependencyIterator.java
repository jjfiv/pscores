package com.github.jjfiv.pscores.scoring;

import com.github.jjfiv.pscores.PositionalScoreArray;
import com.github.jjfiv.pscores.PositionalScoreIterator;
import gnu.trove.list.array.TDoubleArrayList;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;

import java.io.IOException;

/**
 * @author jfoley
 */
public class TemporalDependencyIterator extends AbstractPositionalScoreIterator {
  public double scoreThreshold;
  TDoubleArrayList hits, windowHits;
  public final int width;

  public TemporalDependencyIterator(NodeParameters parameters, PositionalScoreIterator[] children) throws IOException {
    super(parameters, children);
    this.scoreThreshold = parameters.get("scoreThreshold", 0.5);
    this.width = (int) parameters.get("windowSize", 3);

    // collect fast-tf array
    hits = new TDoubleArrayList(children.length);
    for (PositionalScoreIterator ignored : children) {
      hits.add(0);
    }
    windowHits = new TDoubleArrayList();
    for (int i = 0; i < width; i++) {
      windowHits.add(0);
    }

    calculateStatistics();
  }

  @Override
  public double tf(ScoringContext c) {
    int length = length(c);
    fetchArrays(c);

    double score = 0.0;

    for(int shift=0; shift<length-width; shift++) {
      for (int i = 0; i < arrays.length; i++) {
        PositionalScoreArray array = arrays[i];
        hits.setQuick(i, 0);

        if (array == null || array.isEmpty()) continue;

        for (int pos = 0; pos < width; pos++) {
          windowHits.set(pos, 0.0);
          if(pos < array.scores.size()) {
            windowHits.set(pos, array.scores.getQuick(pos));
          }
        }

        // take highest hit in window; max is the or over the possibilities
        hits.setQuick(i, scoreOccurrences(windowHits, "max", scoreThreshold));
      }
      score += scoreOccurrences(hits, this.operation, this.scoreThreshold);
    }

    return score;
  }

}
