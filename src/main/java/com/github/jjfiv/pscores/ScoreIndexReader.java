package com.github.jjfiv.pscores;

import org.lemurproject.galago.core.index.KeyIterator;
import org.lemurproject.galago.core.index.KeyListReader;
import org.lemurproject.galago.core.index.source.ScoreSource;
import org.lemurproject.galago.core.retrieval.iterator.BaseIterator;
import org.lemurproject.galago.core.retrieval.iterator.ScoreIterator;
import org.lemurproject.galago.core.retrieval.iterator.disk.DiskScoreIterator;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeType;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.utility.btree.BTreeIterator;
import org.lemurproject.galago.utility.btree.BTreeReader;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/**
 * @author jfoley.
 */
public class ScoreIndexReader extends KeyListReader {
	public ScoreIndexReader(BTreeReader reader) throws IOException {
		super(reader);
	}
	public ScoreIndexReader(String filename) throws IOException {
		super(filename);
	}

	public static String getDefaultOperator() {
		return "scores";
	}

	@Override
	public Map<String, NodeType> getNodeTypes() {
		return Collections.singletonMap(getDefaultOperator(), new NodeType(ScoreIterator.class));
	}

	@Override
	public KeyIterator getIterator() throws IOException {
		return new KeyIter(this.reader);
	}

	@Override
	public BaseIterator getIterator(Node node) throws IOException {
		if(node.getOperator().equals(getDefaultOperator())) {
			int term;
			if(node.getNodeParameters().isLong("default")) {
				term = (int) node.getNodeParameters().getLong("default");
			} else {
				term = Integer.parseInt(node.getDefaultParameter());
			}

			BTreeIterator iterator = reader.getIterator(Utility.fromInt(term));
			if(iterator == null) return null;
			return new DiskScoreIterator(new ScoreIndexSource(iterator));
		}
		throw new UnsupportedOperationException();
	}

	public static class KeyIter extends KeyValueIterator {

		public KeyIter(BTreeReader reader) throws IOException {
			super(reader);
		}

		@Override
		public String getKeyString() throws IOException {
			return Integer.toString(Utility.toInt(iterator.getKey()));
		}

		@Override
		public String getValueString() throws IOException {
			throw new UnsupportedOperationException();
		}

		@Override
		public ScoreIterator getValueIterator() throws IOException {
			return new DiskScoreIterator(getValueSource());
		}

		public ScoreSource getValueSource() {
			return new ScoreIndexSource(iterator);
		}
	}
}
