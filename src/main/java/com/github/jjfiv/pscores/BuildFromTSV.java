package com.github.jjfiv.pscores;

import gnu.trove.map.hash.TObjectIntHashMap;
import org.lemurproject.galago.tupleflow.Sorter;
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
public class BuildFromTSV {

	public static void buildIndex(Parameters argp) throws IOException {
		String input = argp.getString("input");
		String output = argp.getString("output");

		int nextDocumentId = 0;
		TObjectIntHashMap<String> documentNames = new TObjectIntHashMap<>();

		try (
			Sorter<TermDocPositionScore> sortMe = new Sorter<>(new TermDocPositionScore.TermDocPositionOrder());
			BufferedReader reader = new BufferedReader(new InputStreamReader(StreamCreator.openInputStream(input), "UTF-8"))
		) {
			PositionScoreIndexWriter indexWriter = new PositionScoreIndexWriter(output, Parameters.create());
			sortMe.setProcessor(new TermDocPositionScore.TermDocPositionOrder.TupleShredder(indexWriter));

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
		BuildFromTSV.buildIndex(Arguments.parse(args));
	}

}
