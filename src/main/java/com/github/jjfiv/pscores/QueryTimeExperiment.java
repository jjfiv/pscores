package com.github.jjfiv.pscores;

import com.github.jjfiv.pscores.scoring.IndependentIterator;
import com.github.jjfiv.pscores.util.RandUtil;
import org.lemurproject.galago.core.index.IndexPartReader;
import org.lemurproject.galago.core.index.KeyIterator;
import org.lemurproject.galago.core.index.disk.DiskIndex;
import org.lemurproject.galago.core.retrieval.iterator.ScoreCombinationIterator;
import org.lemurproject.galago.core.retrieval.iterator.ScoreIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
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

	public static FixedSizeMinHeap<IntDocResult> makeHeap(int depth) {
		return new FixedSizeMinHeap<>(IntDocResult.class, depth, new Comparator<IntDocResult>() {
			@Override
			public int compare(IntDocResult lhs, IntDocResult rhs) {
				return -CmpUtil.compare(lhs.score, rhs.score);
			}
		});
	}

	public static int runQueryCountTopK(List<ScoreIterator> children) throws IOException {
		FixedSizeMinHeap<IntDocResult> heap = makeHeap(1000);
		ScoreIterator iter = new ScoreCombinationIterator(new NodeParameters(), children.toArray(new ScoreIterator[children.size()]));
		ScoringContext ctx = new ScoringContext();
		while(!iter.isDone()) {
			ctx.document = iter.currentCandidate();
			iter.syncTo(ctx.document);
			double score = iter.score(ctx);
			heap.offer(new IntDocResult((int) ctx.document, score));
			iter.movePast(ctx.document);
		}
		return heap.size();
	}

	private static void runAOTQueries(ScoreIndexReader reader) throws IOException {
		Set<Integer> concepts = collectAllKeys(reader.getIterator());
		System.out.println("Flattened Index: |concepts|="+concepts.size());

		for (int i = 0; i < 100; i++) {
			List<Integer> randConcepts = RandUtil.sampleRandomly(concepts, 20, new Random(13));

			List<ScoreIterator> children = new ArrayList<>();
			for (Integer randConcept : randConcepts) {
				Node scores = new Node("scores", randConcept.toString());
				children.add((ScoreIterator) reader.getIterator(scores));
			}

			long start = System.currentTimeMillis();
			int heap_size = runQueryCountTopK(children);
			long end = System.currentTimeMillis();

			System.out.println(end-start);
			/*System.out.println(Parameters.parseArray(
				"numResults", heap_size,
				"time_ms", end - start,
				"n", randConcepts.size()
			));*/
		}

	}

	private static void runJITQueries(PositionScoreIndexReader reader) throws IOException {
		Set<Integer> concepts = collectAllKeys(reader.getIterator());
		System.out.println("Full Index: |concepts|="+concepts.size());


		for (int i = 0; i < 100; i++) {
			List<Integer> randConcepts = RandUtil.sampleRandomly(concepts, 20, new Random(13));

			List<ScoreIterator> children = new ArrayList<>();
			for (Integer randConcept : randConcepts) {
				Node scores = new Node("pscores", randConcept.toString());
				NodeParameters np = new NodeParameters();
				np.set("scoreThreshold", 0.5);
				ScoreIterator iter = new IndependentIterator(np,  (PositionalScoreIterator) reader.getIterator(scores));
				children.add(iter);
			}

			long start = System.currentTimeMillis();
			int heap_size = runQueryCountTopK(children);
			long end = System.currentTimeMillis();

			System.out.println(end-start);
			/*System.out.println(Parameters.parseArray(
				"numResults", heap_size,
				"time_ms", end - start,
				"n", randConcepts.size()
			));*/
		}
	}
}
