package com.github.jjfiv.pscores;

import gnu.trove.map.hash.TIntIntHashMap;
import org.lemurproject.galago.core.btree.simple.DiskMapSortedBuilder;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.utility.tools.AppFunction;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author jfoley.
 */
public class MergePScoreIndex extends AppFunction {
  @Override
  public String getName() {
    return "merge-pscore-index";
  }

  @Override
  public String getHelpString() {
    return makeHelpStr(
      "output", "path to new pscores part",
      "indices", "a list of originating pscores parts, with those on the right overriding those on the left"
    );
  }

  @Override
  public void run(Parameters argp, PrintStream stdout) throws Exception {
    String outputPath = argp.getString("output");
    List<String> pscoresInput = argp.getAsList("indices", String.class);

    List<PositionScoreIndexReader> indexReaders = new ArrayList<>();
    for (String pscoresPath : pscoresInput) {
      indexReaders.add(new PositionScoreIndexReader(pscoresPath));
    }

    TIntIntHashMap codeToIndexReader = new TIntIntHashMap();
    for (int i = 0; i < indexReaders.size(); i++) {
      PositionScoreIndexReader indexReader = indexReaders.get(i);
      PositionScoreIndexReader.KeyIterator iter = indexReader.getIterator();
      while(!iter.isDone()) {
        codeToIndexReader.put(Integer.parseInt(iter.getKeyString()), i);
        iter.nextKey();
      }
    }

    int[] concepts = codeToIndexReader.keys();
    Arrays.sort(concepts);

    Parameters opts = Parameters.create();
    opts.put("writerClass", getClass().getName());
    opts.put("readerClass", PositionScoreIndexReader.class.getName());
    opts.put("defaultOperator", PositionScoreIndexReader.getDefaultOperator());
    try(DiskMapSortedBuilder output = new DiskMapSortedBuilder(outputPath, opts)) {
      for (int concept : concepts) {
        System.err.println("# build "+concept);
        output.put(Utility.fromInt(concept), indexReaders.get(codeToIndexReader.get(concept)).getValue(concept));
      }
    }
  }
}
