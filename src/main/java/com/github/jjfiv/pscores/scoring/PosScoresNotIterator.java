package com.github.jjfiv.pscores.scoring;

import com.github.jjfiv.pscores.PositionalScoreArray;
import com.github.jjfiv.pscores.PositionalScoreIterator;
import org.lemurproject.galago.core.retrieval.iterator.ScoreIterator;
import org.lemurproject.galago.core.retrieval.iterator.TransformIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.utility.CmpUtil;
import org.lemurproject.galago.utility.MathUtils;

import java.io.IOException;

/**
 * @author jfoley
 */
public class PosScoresNotIterator extends TransformIterator implements ScoreIterator {

  private final PositionalScoreIterator inner;
  private double collectionScore;

  public PosScoresNotIterator(NodeParameters parameters, PositionalScoreIterator iterator) throws IOException {
    super(iterator);
    this.inner = iterator;

    // collect stats
    this.collectionScore = 0.0;
    ScoringContext statsCtx = new ScoringContext();
    while(!this.isDone()) {
      statsCtx.document = this.currentCandidate();
      collectionScore += (1.0 - inner.data(statsCtx).scores.max());
      this.movePast(statsCtx.document);
    }
    this.reset();
  }

  @Override
  public double score(ScoringContext c) {
    double tf = 0.0;
    final PositionalScoreArray data = inner.data(c);
    if(!data.isEmpty()) {
      tf = (1.0 - data.scores.max());
    }
    final double prob = tf / collectionScore;
    if (prob <= 0 || Double.isNaN(prob) || Double.isInfinite(prob))
      return Math.log(Double.MIN_VALUE);
    return Math.log(MathUtils.clamp(prob, CmpUtil.epsilon, 1.0));
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
  public AnnotatedNode getAnnotatedNode(ScoringContext sc) throws IOException {
    return null;
  }
}
