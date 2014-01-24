package crunch;
import static org.apache.crunch.types.writable.Writables.strings;
import static org.apache.crunch.types.writable.Writables.tableOf;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;

import org.junit.Test;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.lib.Cogroup;
import org.apache.crunch.lib.Join;
import com.google.common.base.Splitter;

public class CogroupCrunchTest implements Serializable {
  
  @Test
  public void test() throws IOException {
    Pipeline pipeline = new MRPipeline(CogroupCrunchTest.class);
    PCollection<String> a = pipeline.readTextFile("join/A");
    PCollection<String> b = pipeline.readTextFile("join/B");
    
    PTable<String, String> aTable = a.parallelDo(new DoFn<String, Pair<String, String>>() {
		@Override
		public void process(String input, Emitter<Pair<String, String>> emitter) {
			Iterator<String> split = Splitter.on('\t').split(input).iterator();
			emitter.emit(Pair.of(split.next(), split.next()));
		}
	}, tableOf(strings(),strings()));

    PTable<String, String> bTable = b.parallelDo(new DoFn<String, Pair<String, String>>() {
		@Override
		public void process(String input, Emitter<Pair<String, String>> emitter) {
			Iterator<String> split = Splitter.on('\t').split(input).iterator();
			String l = split.next();
			String r = split.next();
			emitter.emit(Pair.of(r, l));
		}
	}, tableOf(strings(),strings()));
    
    PTable<String, Pair<Collection<String>, Collection<String>>> cogroup = Cogroup.cogroup(aTable, bTable);
    
    pipeline.writeTextFile(cogroup, "output-cogrouped");
    pipeline.run();
  }

}
