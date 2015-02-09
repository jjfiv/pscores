package com.github.jjfiv.pscores;

import org.junit.Test;
import org.lemurproject.galago.core.index.IndexPartReader;
import org.lemurproject.galago.core.index.disk.DiskIndex;
import org.lemurproject.galago.core.retrieval.iterator.BaseIterator;
import org.lemurproject.galago.core.retrieval.iterator.ScoreIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.utility.Parameters;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

import static org.junit.Assert.*;

public class ScoreIndexWriterTest {

	public static class TemporaryFile implements Closeable {
		File tmp;

		public TemporaryFile(String suffix) throws IOException {
			tmp = File.createTempFile("tmpf", suffix);
		}

		File get() {
			return tmp;
		}

		String getPath() {
			return get().getAbsolutePath();
		}

		@Override
		public void close() throws IOException {
			boolean status = tmp.delete();
			assertTrue(status);
		}
	}

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
				assertEquals(0.1, x.score(ctx), 0.0001);

			}

		}

	}
}