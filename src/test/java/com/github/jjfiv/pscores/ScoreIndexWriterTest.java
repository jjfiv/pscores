package com.github.jjfiv.pscores;

import org.junit.Test;
import org.lemurproject.galago.core.index.IndexPartReader;
import org.lemurproject.galago.core.index.disk.DiskIndex;
import org.lemurproject.galago.core.retrieval.iterator.BaseIterator;
import org.lemurproject.galago.core.retrieval.iterator.ScoreIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.utility.Parameters;

import static org.junit.Assert.*;

public class ScoreIndexWriterTest {

	@Test
	public void testProcessTuple() throws Exception {
		try (TemporaryFile fp = new TemporaryFile(".scores")) {
			String path = fp.getPath();
			assertNotNull(path);

			try (ScoreIndexWriter siw = new ScoreIndexWriter(path, Parameters.create())) {

				siw.processTerm(13);

				siw.processDoc(1);
				siw.processTuple(0.1);

				siw.processDoc(2);
				siw.processTuple(0.2);

				siw.processDoc(3);
				siw.processTuple(1.0);

				siw.processDoc(4);
				siw.processTuple(0.7);
			}

			try (IndexPartReader ipr = DiskIndex.openIndexPart(path)) {
				assertTrue(ipr instanceof ScoreIndexReader);
				ScoreIndexReader reader = (ScoreIndexReader) ipr;

				Node scores = new Node("scores", Integer.toString(13));
				BaseIterator baseIter = reader.getIterator(scores);
				assertTrue(baseIter instanceof ScoreIterator);

				ScoringContext ctx = new ScoringContext();
				ScoreIterator x = (ScoreIterator) baseIter;
				assertEquals(0.1, x.minimumScore(), 0.0001);
				assertEquals(1.0, x.maximumScore(), 0.0001);


				assertFalse(x.isDone());

				ctx.document = 1;
				x.syncTo(ctx.document);
				assertFalse(x.isDone());
				assertEquals(0.1, x.score(ctx), 0.0001);

				ctx.document = 2;
				x.syncTo(ctx.document);
				assertFalse(x.isDone());
				assertEquals(0.2, x.score(ctx), 0.0001);

				ctx.document = 3;
				x.syncTo(ctx.document);
				assertFalse(x.isDone());
				assertEquals(1.0, x.score(ctx), 0.0001);

				ctx.document = 4;
				x.syncTo(ctx.document);
				assertFalse(x.isDone());
				assertEquals(0.7, x.score(ctx), 0.0001);

				x.movePast(4);
				assertTrue(x.isDone());
			}

		}

	}
}