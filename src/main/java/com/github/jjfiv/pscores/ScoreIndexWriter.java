package com.github.jjfiv.pscores;

import org.lemurproject.galago.core.btree.simple.DiskMapSortedBuilder;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.buffer.DiskSpillCompressedByteBuffer;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.utility.btree.IndexElement;
import org.lemurproject.galago.utility.buffer.VByteOutput;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * @author jfoley.
 */
public class ScoreIndexWriter implements TermDocScore.TermDocOrder.ShreddedProcessor {

	private final DiskMapSortedBuilder builder;

	public ScoreIndexWriter(TupleFlowParameters tfp) throws IOException {
		this(tfp.getJSON().getString("filename"), tfp.getJSON());
	}

	public ScoreIndexWriter(String fileName, Parameters opts) throws IOException {
		opts.put("writerClass", getClass().getName());
		opts.put("readerClass", ScoreIndexReader.class.getName());
		opts.put("defaultOperator", ScoreIndexReader.getDefaultOperator());
		builder = new DiskMapSortedBuilder(fileName, opts);
	}

	@Override
	public void processTerm(int term) throws IOException {

	}

	@Override
	public void processDoc(int doc) throws IOException {

	}

	@Override
	public void processTuple(double score) throws IOException {

	}

	@Override
	public void close() throws IOException {

	}

	public class DocsAndScores implements IndexElement {
		private final byte[] term;
		private final DiskSpillCompressedByteBuffer buffer;
		private final VByteOutput data;

		public DocsAndScores(int term) {
			this.term = Utility.fromInt(term);
			this.buffer = new DiskSpillCompressedByteBuffer();
			this.data = new VByteOutput(new DataOutputStream(buffer));
		}

		@Override
		public byte[] key() {
			return term;
		}

		@Override
		public long dataLength() {
			return 4 + buffer.length();
		}

		@Override
		public void write(OutputStream stream) throws IOException {

		}
	}
}
