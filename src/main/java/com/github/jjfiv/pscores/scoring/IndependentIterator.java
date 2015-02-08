package com.github.jjfiv.pscores.scoring;

import com.github.jjfiv.pscores.PositionalScoreArray;
import com.github.jjfiv.pscores.PositionalScoreIterator;
import gnu.trove.list.array.TDoubleArrayList;
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
public class IndependentIterator extends TransformIterator implements ScoreIterator {

  private final PositionalScoreIterator inner;
  private final double scoreThreshold;
  private double collectionScore;
  public final String operation;

  public IndependentIterator(NodeParameters parameters, PositionalScoreIterator iterator) throws IOException {
    super(iterator);
    this.inner = iterator;
    this.scoreThreshold = parameters.get("scoreThreshold", 0.1);
    this.operation = parameters.get("op", "max");

    // collect stats
    this.collectionScore = 0.0;
    ScoringContext statsCtx = new ScoringContext();
    while(!this.isDone()) {
      statsCtx.document = this.currentCandidate();
      double score;
      TDoubleArrayList scores = inner.data(statsCtx).scores;
      switch (operation) {
        case "max":
          score = scores.max();
          break;
        case "mean":
          score = scores.sum() / scores.size();
          break;
        default: throw new IllegalArgumentException("independentOp="+ operation);
      }
      if(score > scoreThreshold) {
        collectionScore += score;
      }
      this.movePast(statsCtx.document);
    }
    this.reset();
  }

  @Override
  public double score(ScoringContext c) {
    double tf = 0.0;
    final PositionalScoreArray data = inner.data(c);
    if(!data.isEmpty()) {
      tf = data.scores.max();
    }
    if(tf < scoreThreshold)
      return Math.log(Double.MIN_VALUE);

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
