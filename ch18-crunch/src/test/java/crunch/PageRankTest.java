package crunch;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import java.io.File;
import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.MapFn;
import org.apache.crunch.PObject;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineExecution;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.lib.Aggregate;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.types.avro.Avros;
import org.apache.hadoop.conf.Configuration;
import org.junit.Rule;
import org.junit.Test;

import static org.apache.crunch.types.avro.Avros.floats;
import static org.apache.crunch.types.avro.Avros.strings;
import static org.apache.crunch.types.avro.Avros.tableOf;
import static org.junit.Assert.assertEquals;

public class PageRankTest implements Serializable {

  @Rule
  public transient TemporaryPath tmpDir = new TemporaryPath();

  public static class PageRankData {
    public float score;
    public float lastScore;
    public List<String> urls;

    public PageRankData() {
    }

    public PageRankData(float score, float lastScore, Iterable<String> urls) {
      this.score = score;
      this.lastScore = lastScore;
      this.urls = Lists.newArrayList(urls);
    }

    public PageRankData next(float newScore) {
      return new PageRankData(newScore, score, urls);
    }

    public float propagatedScore() {
      return score / urls.size();
    }

    @Override
    public String toString() {
      return score + " " + lastScore + " " + urls;
    }
  }

  public PTable<String, PageRankData> pageRank(PTable<String, PageRankData> input, final float d) {
    PTable<String, Float> outbound = input.parallelDo(new DoFn<Pair<String,
        PageRankData>, Pair<String, Float>>() {
      @Override
      public void process(Pair<String, PageRankData> input, Emitter<Pair<String,
          Float>> emitter) {
        PageRankData prd = input.second();
        for (String link : prd.urls) {
          emitter.emit(Pair.of(link, prd.propagatedScore()));
        }
      }
    }, tableOf(strings(), floats()));

    return input.cogroup(outbound).mapValues(
        new MapFn<Pair<Collection<PageRankData>, Collection<Float>>, PageRankData>() {
          @Override
          public PageRankData map(Pair<Collection<PageRankData>, Collection<Float>> input) {
            PageRankData prd = Iterables.getOnlyElement(input.first());
            Collection<Float> propagatedScores = input.second();
            float sum = 0.0f;
            for (Float s : propagatedScores) {
              sum += s;
            }
            return prd.next(d + (1.0f - d) * sum);
          }
        }, input.getValueType());
  }

  private PObject<Float> computeDelta(PTable<String, PageRankData> scores) {
    return Aggregate.max(scores.parallelDo(new MapFn<Pair<String, PageRankData>, Float>() {
      @Override
      public Float map(Pair<String, PageRankData> input) {
        PageRankData prd = input.second();
        return Math.abs(prd.score - prd.lastScore);
      }
    }, floats()));
  }

  private PTable<String, PageRankData> readUrls(Pipeline pipeline, String urlInput) {
    return pipeline.readTextFile(urlInput)
        .parallelDo(new MapFn<String, Pair<String, String>>() {
          @Override
          public Pair<String, String> map(String input) {
            String[] urls = input.split("\\t");
            return Pair.of(urls[0], urls[1]);
          }
        }, tableOf(strings(), strings())).groupByKey()
        .mapValues(new MapFn<Iterable<String>, PageRankData>() {
          @Override
          public PageRankData map(Iterable<String> input) {
            return new PageRankData(1.0f, 0.0f, input);
          }
        }, Avros.reflects(PageRankData.class));
  }

  @Test
  public void test() throws Exception {
    String urlInput = tmpDir.copyResourceFileName("urls.txt");
    Pipeline pipeline = new MRPipeline(getClass());
    PTable<String, PageRankData> scores = readUrls(pipeline, urlInput);
    Float delta = 1.0f;
    while (delta > 0.01) {
      scores = pageRank(scores, 0.5f);
      scores.materialize().iterator(); // force scores to be materialized
      //writeDotFile(pipeline.getConfiguration().get("crunch.planner.dotfile"));
      PObject<Float> pDelta = computeDelta(scores);
      delta = pDelta.getValue();
      //writeDotFile(pipeline.getConfiguration().get("crunch.planner.dotfile"));
    }
    pipeline.done();
    assertEquals(0.0048, delta, 0.001);
  }

  static int i = 0;
  private void run(Pipeline pipeline) throws Exception {
    PipelineExecution execution = pipeline.runAsync();
    writeDotFile(execution.getPlanDotFile());
    execution.waitUntilDone();
  }

  private void writeDotFile(String dotfile) throws Exception {
    FileUtils.write(new File("target/pagerank-" + i++ + ".dot"), dotfile, "UTF-8");
  }
}
