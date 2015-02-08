package com.github.jjfiv.pscores.scoring;

import org.lemurproject.galago.core.retrieval.iterator.ConjunctionIterator;
import org.lemurproject.galago.core.retrieval.iterator.ScoreIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.utility.MathUtils;

import java.io.IOException;
import java.util.Arrays;

/**
 * @author jfoley
 */
public class GeometricMeanIterator extends ConjunctionIterator implements ScoreIterator {

  private final ScoreIterator[] children;
  private final double[] values, weights;

  public GeometricMeanIterator(NodeParameters parameters, ScoreIterator[] queryIterators) {
    super(parameters, queryIterators);
    this.children = queryIterators;
    this.values = new double[children.length];
    this.weights = new double[children.length];
    Arrays.fill(weights, 1.0);
  }

  @Override
  public String getValueString(ScoringContext sc) throws IOException {
    return null;
  }

  @Override
  public AnnotatedNode getAnnotatedNode(ScoringContext sc) throws IOException {
    return null;
  }

  @Override
  public double score(ScoringContext c) {
    for (int i = 0; i < children.length; i++) {
      values[i] = children[i].score(c);
    }
    return MathUtils.logWeightedGeometricMean(weights, values);
  }

  @Override
  public double maximumScore() {
    double[] tmp = new double[children.length];
    for (int i = 0; i < children.length; i++) {
      tmp[i] = children[i].maximumScore();
    }
    return MathUtils.logWeightedGeometricMean(weights, tmp);
  }

  @Override
  public double minimumScore() {
    double[] tmp = new double[children.length];
    for (int i = 0; i < children.length; i++) {
      tmp[i] = children[i].minimumScore();
    }
    return MathUtils.logWeightedGeometricMean(weights, tmp);
  }
}
