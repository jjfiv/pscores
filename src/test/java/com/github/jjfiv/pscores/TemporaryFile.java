package com.github.jjfiv.pscores;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertTrue;

/**
* @author jfoley.
*/
public class TemporaryFile implements Closeable {
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
