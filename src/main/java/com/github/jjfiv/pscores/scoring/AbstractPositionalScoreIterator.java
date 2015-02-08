package com.github.jjfiv.pscores.scoring;

import com.github.jjfiv.pscores.PositionalScoreArray;
import com.github.jjfiv.pscores.PositionalScoreIterator;
import gnu.trove.list.array.TDoubleArrayList;
import org.lemurproject.galago.core.retrieval.iterator.ConjunctionIterator;
import org.lemurproject.galago.core.retrieval.iterator.ScoreIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.utility.MathUtils;

import java.io.IOException;

/**
 * @author jfoley
 */
public abstract class AbstractPositionalScoreIterator extends ConjunctionIterator implements ScoreIterator {
  public final PositionalScoreIterator[] children;
  public PositionalScoreArray[] arrays;
  public final String operation;
  public double collectionScore;
  public double maxCollectionScore;

  public AbstractPositionalScoreIterator(NodeParameters parameters, PositionalScoreIterator[] queryIterators) {
    super(parameters, queryIterators);
    this.children = queryIterators;
    this.arrays = new PositionalScoreArray[children.length];
    this.operation = parameters.get("op", "and");
  }

  /** call this in the constructor */
  public void calculateStatistics() throws IOException {
    // calculate statistics
    this.collectionScore = 0.0;
    ScoringContext statsCtx = new ScoringContext();
    while(!this.isDone()) {
      statsCtx.document = this.currentCandidate();
      collectionScore += tf(statsCtx);
      maxCollectionScore += length(statsCtx);
      this.movePast(statsCtx.document);
    }
    //System.err.println(this.getClass().getSimpleName()+" op: "+operation+" mean: "+microAverage+" max: "+microMax+" collectionScore: "+collectionScore);
    this.reset();
  }

  public abstract double tf(ScoringContext c);

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

  public void fetchArrays(ScoringContext c) {
    // load up these arrays...
    for (int i = 0; i < children.length; i++) {
      PositionalScoreIterator child = children[i];
      arrays[i] = child.data(c);
    }
  }

  public static double scoreOccurrences(TDoubleArrayList hits, String operation, double threshold) {
    double frameScore = 0;
    switch(operation) {
      case "max":
        frameScore = hits.max();
        break;
      case "min":
        frameScore = hits.max();
        break;
      case "and":
        frameScore = 0;
        for (int i = 0; i < hits.size(); i++) {
          frameScore += Math.log(hits.getQuick(i));
        }
        frameScore = Math.exp(frameScore);
        break;
      case "mean":
        frameScore = hits.sum() / hits.size();
        break;
    }
    if (frameScore > threshold) { // hit threshold
      return MathUtils.clamp(frameScore, threshold, 1);
    }
    return 0.0;
  }



  @Override
  public double score(ScoringContext c) {
    double score = tf(c);

    // maximum value...
    score /= collectionScore;
    if (score <= 0 || Double.isNaN(score) || Double.isInfinite(score))
      return Math.log(Double.MIN_VALUE);

    return Math.log(MathUtils.clamp(score, Double.MIN_VALUE, 1.0));
  }

  @Override
  public double maximumScore() {
    return Math.log(1.0);
  }

  @Override
  public double minimumScore() {
    return Math.log(Double.MIN_VALUE);
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
