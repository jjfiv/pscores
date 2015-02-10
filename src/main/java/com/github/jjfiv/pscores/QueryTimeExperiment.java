package com.github.jjfiv.pscores;

import com.github.jjfiv.pscores.util.RandUtil;
import org.lemurproject.galago.core.index.IndexPartReader;
import org.lemurproject.galago.core.index.KeyIterator;
import org.lemurproject.galago.core.index.disk.DiskIndex;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.utility.tools.Arguments;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

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

	private static void runAOTQueries(ScoreIndexReader reader) throws IOException {
		Set<Integer> concepts = collectAllKeys(reader.getIterator());
		System.out.println("Flattened Index: |concepts|="+concepts.size());

		List<Integer> randConcepts = RandUtil.sampleRandomly(concepts, 30, new Random(13));

		System.out.println(randConcepts);
	}

	private static void runJITQueries(PositionScoreIndexReader reader) throws IOException {
		Set<Integer> concepts = collectAllKeys(reader.getIterator());
		System.out.println("Full Index: |concepts|="+concepts.size());
		List<Integer> randConcepts = RandUtil.sampleRandomly(concepts, 30, new Random(13));

		System.out.println(randConcepts);
	}
}
