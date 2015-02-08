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
public class SpatialDependencyIterator extends AbstractPositionalScoreIterator {
  public double scoreThreshold;
  public int countThreshold;
  TDoubleArrayList hits;

  public SpatialDependencyIterator(NodeParameters parameters, PositionalScoreIterator[] children) throws IOException {
    super(parameters, children);
    this.scoreThreshold = parameters.get("scoreThreshold", 0.5);
    this.countThreshold = (int) parameters.get("countThreshold", 2);

    // collect fast-tf array
    hits = new TDoubleArrayList(children.length);
    for (PositionalScoreIterator ignored : children) {
      hits.add(0);
    }

    // calculate statistics
    calculateStatistics();
  }

  public double tf(ScoringContext c) {
    int length = length(c);

    double score = 0.0;

    for(int pos=0; pos<length; pos++) {
      for (int i = 0; i < arrays.length; i++) {
        PositionalScoreArray array = arrays[i];
        hits.setQuick(i, 0);

        if (array == null || array.isEmpty()) continue;

        double cscore = 0.0;
        if(pos < array.scores.size()) {
          cscore = array.scores.getQuick(pos);
        }
        hits.setQuick(i, cscore);
      }

      score += scoreOccurrences(hits, this.operation, this.scoreThreshold);
    }

    return score;
  }
}
