package com.github.jjfiv.pscores;

import org.lemurproject.galago.core.index.source.BTreeValueSource;
import org.lemurproject.galago.core.index.source.ScoreSource;
import org.lemurproject.galago.utility.btree.BTreeIterator;
import org.lemurproject.galago.utility.buffer.VByteInput;

import java.io.IOException;

/**
 * @author jfoley.
 */
public class ScoreIndexSource extends BTreeValueSource implements ScoreSource {
	private final double defaultScore;
	private final double minScore;
	private final double maxScore;
	VByteInput data;
	boolean hasCurrent;
	long docId;
	double score;


	public ScoreIndexSource(BTreeIterator iterator) throws IOException {
		super(iterator);
		this.defaultScore = iterator.getManifest().get("defaultScore", 0.0);
		this.minScore = iterator.getManifest().get("minimumScore", Double.MIN_VALUE);
		this.maxScore = iterator.getManifest().get("maximumScore", Double.MAX_VALUE);
		initialize();
	}

	private void initialize() throws IOException {
		data = new VByteInput(btreeIter.getValueStream());
		hasCurrent = true;
		score = defaultScore;
		docId = 0;
		readNextDocument();
	}

	private void readNextDocument() throws IOException {
		if(!hasCurrent) return;
		docId += data.readLong();
	}

	@Override
	public double score(long id) {
		return 0;
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

	@Override
	public boolean isDone() {
		return false;
	}

	@Override
	public boolean hasAllCandidates() {
		return false;
	}

	@Override
	public long totalEntries() {
		return 0;
	}

	@Override
	public long currentCandidate() {
		return 0;
	}

	@Override
	public void movePast(long id) throws IOException {

	}

	@Override
	public void syncTo(long id) throws IOException {

	}
}
