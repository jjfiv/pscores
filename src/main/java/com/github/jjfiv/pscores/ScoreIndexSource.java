package com.github.jjfiv.pscores;

import org.lemurproject.galago.core.index.source.BTreeValueSource;
import org.lemurproject.galago.core.index.source.ScoreSource;
import org.lemurproject.galago.utility.btree.BTreeIterator;
import org.lemurproject.galago.utility.buffer.DataStream;
import org.lemurproject.galago.utility.buffer.VByteInput;

import java.io.IOException;

/**
 * @author jfoley.
 */
public class ScoreIndexSource extends BTreeValueSource implements ScoreSource {
	private final double defaultScore;
	private double minScore;
	private double maxScore;
	VByteInput data;
	boolean hasCurrent;
	long docId;
	double score;
	private int totalDocs;
	private int docIndex;


	public ScoreIndexSource(BTreeIterator iterator) throws IOException {
		super(iterator);
		this.defaultScore = iterator.getManifest().get("defaultScore", 0.0);
		this.minScore = iterator.getManifest().get("minimumScore", Double.MIN_VALUE);
		this.maxScore = iterator.getManifest().get("maximumScore", Double.MAX_VALUE);
		initialize();
	}

	private void initialize() throws IOException {
		DataStream valueStream = btreeIter.getValueStream();
		data = new VByteInput(valueStream);
		hasCurrent = true;
		score = defaultScore;
		docId = 0;
		docIndex = 0;
		totalDocs = valueStream.readInt();
		minScore = valueStream.readDouble();
		maxScore = valueStream.readDouble();
		readNextDocument();
	}

	private void readNextDocument() throws IOException {
		if(!isDone()) {
			docIndex++;
			docId += data.readInt();
			score = data.readDouble();
		} else {
			docId = Integer.MAX_VALUE;
		}
	}

	@Override
	public double score(long id) {
		if(docId == id) {
			return score;
		}
		return Double.MIN_VALUE;
	}

	@Override
	public double maxScore() {
		return maxScore;
	}

	@Override
	public double minScore() {
		return minScore;
	}

	@Override
	public void reset() throws IOException {
		initialize();
	}

	public boolean hasNext() {
		return docIndex < totalDocs;
	}

	@Override
	public boolean isDone() {
		return !hasNext();
	}

	@Override
	public boolean hasAllCandidates() {
		return false;
	}

	@Override
	public long totalEntries() {
		return totalDocs;
	}

	@Override
	public long currentCandidate() {
		return docId;
	}

	@Override
	public void movePast(long id) throws IOException {
		syncTo(id+1);
	}

	@Override
	public void syncTo(long id) throws IOException {
		while(docId < id) {
			readNextDocument();
		}
	}
}
