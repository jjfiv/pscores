package com.github.jjfiv.pscores;

import gnu.trove.map.hash.TObjectIntHashMap;
import org.lemurproject.galago.tupleflow.*;
import org.lemurproject.galago.tupleflow.error.IncompatibleProcessorException;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.utility.StreamCreator;
import org.lemurproject.galago.utility.tools.Arguments;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * @author jfoley.
 */
public class BuildFlatFromTSV {

	public static abstract class ScoreAggregator implements Source<TermDocScore> , TermDocPositionScore.TermDocPositionOrder.ShreddedProcessor {

		private int currentTerm;
		private int currentDocument;
		PositionalScoreArray pscores;

		public ScoreAggregator() {
			currentTerm = -1;
			pscores = new PositionalScoreArray();
		}

		/** aggregation function */
		public abstract Double aggregate(PositionalScoreArray pscores);

		@Override
		public void processTerm(int term) throws IOException {
			flushTerm();
			this.currentTerm = term;
		}

		private void flushTerm() throws IOException {
			if(currentTerm == -1) return;
			flushDocument();
			currentTerm = -1;
		}

		private void flushDocument() throws IOException {
			if(currentDocument == -1) return;
			Double aggregateScore = aggregate(pscores);
			pscores.clear();
			if(aggregateScore == null){
				currentDocument = -1;
				return;
			}

			TermDocScore output = new TermDocScore();
			output.term = currentTerm;
			output.doc = currentDocument;
			output.score = aggregateScore;
			processor.process(output);

			currentDocument = -1;
		}

		@Override
		public void processDoc(int doc) throws IOException {
			flushDocument();
			currentDocument = doc;
		}

		@Override
		public void processPosition(int position) throws IOException {
			pscores.addPosition(position);
		}

		@Override
		public void processTuple(double score) throws IOException {
			pscores.addScore(score);
		}

		public Processor<TermDocScore> processor;

		@Override
		public void setProcessor(Step next) throws IncompatibleProcessorException {
			Linkage.link(this, next);
		}

		@Override
		public void close() throws IOException {
			flushTerm();
			processor.close();
		}
	}

	/** aggregation function used to build single index for now */
	public static class MaxIfAboveHalfAggregator extends ScoreAggregator {
		@Override
		public Double aggregate(PositionalScoreArray pscores) {
			if(pscores.isEmpty()) return null;
			double max = pscores.scores.max();
			if(max < 0.5) return null;
			return max;
		}
	}

	public static void buildIndex(Parameters argp) throws IOException {
		String input = argp.getString("input");
		String output = argp.getString("output");

		int nextDocumentId = 0;
		TObjectIntHashMap<String> documentNames = new TObjectIntHashMap<>();

		try (
			Sorter<TermDocPositionScore> sortMe = new Sorter<>(new TermDocPositionScore.TermDocPositionOrder());
			BufferedReader reader = new BufferedReader(new InputStreamReader(StreamCreator.openInputStream(input), "UTF-8"))
		) {
			ScoreIndexWriter indexWriter = new ScoreIndexWriter(output, Parameters.create());
			MaxIfAboveHalfAggregator agg = new MaxIfAboveHalfAggregator();
			agg.setProcessor(new TermDocScore.TermDocOrder.TupleShredder(indexWriter));
			sortMe.setProcessor(new TermDocPositionScore.TermDocPositionOrder.TupleShredder(agg));

			while(true) {
				String next = reader.readLine();
				if(next == null) break;
				String[] cols = next.split("\t");

				int term = Integer.parseInt(cols[0]);
				String videoId = cols[1];
				int position = Integer.parseInt(cols[2]);
				double score = Double.parseDouble(cols[3]);

				int docId;
				if(!documentNames.containsKey(videoId)) {
					docId = ++nextDocumentId;
					documentNames.put(videoId, docId);
				} else {
					docId = documentNames.get(videoId);
				}

				TermDocPositionScore tdps = new TermDocPositionScore(term, docId, position, score);
				sortMe.process(tdps);
			}
		} catch (IncompatibleProcessorException e) {
			throw new RuntimeException(e);
		}
	}

	public static void main(String[] args) throws IOException {
		BuildFlatFromTSV.buildIndex(Arguments.parse(args));
	}

}
