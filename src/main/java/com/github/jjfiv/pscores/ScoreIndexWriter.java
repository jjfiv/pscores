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
	private DocsAndScores termData;

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
		flushTerm();
		this.termData = new DocsAndScores(term);
	}

	private void flushTerm() throws IOException {
		if(termData == null) return;
		builder.putCustom(termData);
		termData = null;
	}

	@Override
	public void processDoc(int doc) throws IOException {
		termData.addDoc(doc);
	}

	@Override
	public void processTuple(double score) throws IOException {
		termData.addScore(score);
	}

	@Override
	public void close() throws IOException {
		flushTerm();
		builder.close();
	}

	public class DocsAndScores implements IndexElement {
		private final byte[] term;
		private final DiskSpillCompressedByteBuffer buffer;
		private final VByteOutput data;
		private int lastDoc;
		private int docCount;
		private double minScore;
		private double maxScore;

		public DocsAndScores(int term) {
			this.term = Utility.fromInt(term);
			this.buffer = new DiskSpillCompressedByteBuffer();
			this.data = new VByteOutput(new DataOutputStream(buffer));
			this.lastDoc = 0;
			this.docCount = 0;
			this.minScore = Double.MAX_VALUE;
			this.maxScore = Double.MIN_VALUE;
		}

		public void addDoc(int doc) throws IOException {
			data.writeInt(doc - lastDoc);
			docCount++;
			lastDoc = doc;
		}
		public void addScore(double score) throws IOException {
			data.writeDouble(score);
			if(score > maxScore) {
				maxScore = score;
			}
			if(score < minScore) {
				minScore = score;
			}
		}

		@Override
		public byte[] key() {
			return term;
		}

		@Override
		public long dataLength() {
			return 4 + (8*2) + buffer.length();
		}

		@Override
		public void write(OutputStream stream) throws IOException {
			DataOutputStream output = new DataOutputStream(stream);
			output.writeInt(docCount);
			output.writeDouble(minScore);
			output.writeDouble(maxScore);
			buffer.write(output);
		}
	}
}
