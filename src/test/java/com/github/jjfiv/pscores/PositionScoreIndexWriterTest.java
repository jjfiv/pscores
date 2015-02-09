package com.github.jjfiv.pscores;

import org.junit.Test;
import org.lemurproject.galago.core.index.IndexPartReader;
import org.lemurproject.galago.core.index.disk.DiskIndex;
import org.lemurproject.galago.core.retrieval.iterator.BaseIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.utility.Parameters;

import static org.junit.Assert.*;

public class PositionScoreIndexWriterTest {

	@Test
	public void testProcessTuple() throws Exception {
		try (TemporaryFile fp = new TemporaryFile(".pscores")) {
			String path = fp.getPath();
			assertNotNull(path);

			try (PositionScoreIndexWriter psiw = new PositionScoreIndexWriter(path, Parameters.create())) {
				psiw.processTerm(13);

				psiw.processDoc(1);
				psiw.processPosition(0);
				psiw.processTuple(0.0);
				psiw.processPosition(3);
				psiw.processTuple(0.3);
				psiw.processPosition(7);
				psiw.processTuple(0.7);
				psiw.processPosition(10);
				psiw.processTuple(1.0);

				psiw.processDoc(2);
				psiw.processPosition(0);
				psiw.processTuple(0.1);
				psiw.processPosition(1);
				psiw.processTuple(0.1);
				psiw.processPosition(2);
				psiw.processTuple(0.1);

				psiw.processTerm(15);

				psiw.processDoc(3);
				psiw.processPosition(0);
				psiw.processTuple(0.2);
				psiw.processPosition(1);
				psiw.processTuple(0.2);
				psiw.processPosition(2);
				psiw.processTuple(0.2);
			}

			try (IndexPartReader ipr = DiskIndex.openIndexPart(path)) {
				assertTrue(ipr instanceof PositionScoreIndexReader);
				PositionScoreIndexReader reader = (PositionScoreIndexReader) ipr;

				Node scores = new Node("pscores", Integer.toString(13));
				BaseIterator baseIter = reader.getIterator(scores);
				assertTrue(baseIter instanceof PositionalScoreIterator);

				ScoringContext ctx = new ScoringContext();
				PositionalScoreIterator x = (PositionalScoreIterator) baseIter;

				assertFalse(x.isDone());

				ctx.document = 1;
				x.syncTo(ctx.document);
				assertFalse(x.isDone());
				PositionalScoreArray pscores = x.data(ctx);
				assertEquals(4, pscores.positions.size());
				assertEquals(4, pscores.scores.size());
				assertArrayEquals(new double[] {0.0, 0.3, 0.7, 1.0}, pscores.scores.toArray(), 0.001);
				assertArrayEquals(new int[] {0, 3, 7, 10}, pscores.positions.toArray());


			}

		}
	}
}