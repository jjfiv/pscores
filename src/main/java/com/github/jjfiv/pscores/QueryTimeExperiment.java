package com.github.jjfiv.pscores;

import com.github.jjfiv.pscores.util.RandUtil;
import org.lemurproject.galago.core.index.IndexPartReader;
import org.lemurproject.galago.core.index.KeyIterator;
import org.lemurproject.galago.core.index.disk.DiskIndex;
import org.lemurproject.galago.core.retrieval.iterator.ScoreIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.utility.CmpUtil;
import org.lemurproject.galago.utility.FixedSizeMinHeap;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.utility.lists.Scored;
import org.lemurproject.galago.utility.tools.Arguments;

import java.io.IOException;
import java.util.*;

/**
 * @author jfoley.
 */
public class QueryTimeExperiment {

	public static void main(String[] args) throws IOException {
		Parameters argp = Arguments.parse(args);
		String indexPath = argp.getString("index");

		try (IndexPartReader ipr = DiskIndex.openIndexPart(indexPath)) {
			if(ipr instanceof ScoreIndexReader) {
				ScoreIndexReader reader = (ScoreIndexReader) ipr;
				runAOTQueries(reader);
			} else if(ipr instanceof PositionScoreIndexReader) {
				PositionScoreIndexReader reader = (PositionScoreIndexReader) ipr;
				runJITQueries(reader);
			}
		}
	}

	public static Set<Integer> collectAllKeys(KeyIterator iter) throws IOException {
		HashSet<Integer> terms = new HashSet<>();

		while(!iter.isDone()) {
			int k = Utility.toInt(iter.getKey());
			terms.add(k);
			if(!iter.nextKey()) {
				break;
			}
		}
		return terms;
	}

	public static class IntDocResult extends Scored {
		int doc;

		public IntDocResult(int doc, double score) {
			super(score);
			this.doc = doc;
		}

		@Override
		public Scored clone(double score) {
			return new IntDocResult(doc, score);
		}
	}

	private static void runAOTQueries(ScoreIndexReader reader) throws IOException {
		Set<Integer> concepts = collectAllKeys(reader.getIterator());
		System.out.println("Flattened Index: |concepts|="+concepts.size());

		List<Integer> randConcepts = RandUtil.sampleRandomly(concepts, 30, new Random(13));

		System.out.println(randConcepts);

		for (Integer randConcept : randConcepts) {
			long start = System.currentTimeMillis();

			FixedSizeMinHeap<IntDocResult> heap = new FixedSizeMinHeap<>(IntDocResult.class, 200, new Comparator<IntDocResult>() {
				@Override
				public int compare(IntDocResult lhs, IntDocResult rhs) {
					return -CmpUtil.compare(lhs.score, rhs.score);
				}
			});

			Node scores = new Node("scores", randConcept.toString());
			ScoreIterator iter = (ScoreIterator) reader.getIterator(scores);
			ScoringContext ctx = new ScoringContext();
			while(!iter.isDone()) {
				ctx.document = iter.currentCandidate();
				iter.syncTo(ctx.document);
				double score = iter.score(ctx);
				heap.offer(new IntDocResult((int) ctx.document, score));
				iter.movePast(ctx.document);
			}

			long end = System.currentTimeMillis();

			System.out.println(Parameters.parseArray(
				"numResults", heap.size(),
				"time_ms", end - start,
				"concept", randConcept
			));
		}

	}

	private static void runJITQueries(PositionScoreIndexReader reader) throws IOException {
		Set<Integer> concepts = collectAllKeys(reader.getIterator());
		System.out.println("Full Index: |concepts|="+concepts.size());
		List<Integer> randConcepts = RandUtil.sampleRandomly(concepts, 30, new Random(13));

		System.out.println(randConcepts);
	}
}
