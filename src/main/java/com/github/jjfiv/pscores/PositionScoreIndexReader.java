package com.github.jjfiv.pscores;

import org.lemurproject.galago.core.index.KeyListReader;
import org.lemurproject.galago.core.index.source.DataSource;
import org.lemurproject.galago.core.retrieval.iterator.BaseIterator;
import org.lemurproject.galago.core.retrieval.iterator.disk.DiskDataIterator;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeType;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.utility.btree.BTreeIterator;
import org.lemurproject.galago.utility.btree.BTreeReader;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
* @author jfoley
*/
public class PositionScoreIndexReader extends KeyListReader {
  public PositionScoreIndexReader(BTreeReader r) {
    super(r);
  }

  public PositionScoreIndexReader(String filename) throws IOException {
    super(filename);
  }

  @Override
  public Map<String, NodeType> getNodeTypes() {
    HashMap<String,NodeType> types = new HashMap<>();
    types.put(getDefaultOperator(), new NodeType(PositionalScoreIterator.class));
    return types;
  }

  public byte[] getValue(int code) throws IOException {
    return reader.getValueBytes(Utility.fromInt(code));
  }

  @Override
  public KeyIterator getIterator() throws IOException {
    return new KeyIterator(reader);
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
      return new PositionScoreIndexIterator(new PositionalScoreIndexSource(iterator));
    }
    throw new UnsupportedOperationException();
  }

  public static String getDefaultOperator() {
    return "pscores";
  }

  public static class KeyIterator extends KeyValueIterator {
    public KeyIterator(BTreeReader reader) throws IOException {
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

    public DataSource<PositionalScoreArray> getValueSource() throws IOException {
      return new PositionalScoreIndexSource(iterator);
    }

    @Override
    public PositionalScoreIterator getValueIterator() throws IOException {
      return new PositionScoreIndexIterator(getValueSource());
    }
  }

  public static class PositionScoreIndexIterator extends DiskDataIterator<PositionalScoreArray> implements PositionalScoreIterator {

    public PositionScoreIndexIterator(DataSource<PositionalScoreArray> src) {
      super(src);
    }
  }
}
