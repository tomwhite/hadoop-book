/*
 * Copyright 2008 Last.fm.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package fm.last.hadoop.programs.labs.trackstats;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.MultipleInputs;

import fm.last.hadoop.io.records.TrackStats;

/**
 * Program that calculates various track-related statistics from raw listening data.
 */
public class TrackStatisticsProgram {

  public static final Log log = LogFactory.getLog(TrackStatisticsProgram.class);

  // values below indicate position in raw data for each value

  private static final int COL_USERID = 0;
  private static final int COL_TRACKID = 1;
  private static final int COL_SCROBBLES = 2;
  private static final int COL_RADIO = 3;
  private static final int COL_SKIP = 4;

  private Configuration conf;

  /**
   * Constructs a new TrackStatisticsProgram, using a default Configuration.
   */
  public TrackStatisticsProgram() {
    this.conf = new Configuration();
  }

  /**
   * Enumeration for Hadoop error counters.
   */
  private enum COUNTER_KEYS {
    INVALID_LINES, NOT_LISTEN
  };

  /**
   * Mapper that takes in raw listening data and outputs the number of unique listeners per track.
   */
  public static class UniqueListenersMapper extends MapReduceBase implements
      Mapper<LongWritable, Text, IntWritable, IntWritable> {

    public void map(LongWritable position, Text rawLine, OutputCollector<IntWritable, IntWritable> output,
        Reporter reporter) throws IOException {

      String line = (rawLine).toString();
      if (line.trim().isEmpty()) { // if the line is empty, report error and ignore
        reporter.incrCounter(COUNTER_KEYS.INVALID_LINES, 1);
        return;
      }

      String[] parts = line.split(" "); // raw data is whitespace delimited
      try {
        int scrobbles = Integer.parseInt(parts[TrackStatisticsProgram.COL_SCROBBLES]);
        int radioListens = Integer.parseInt(parts[TrackStatisticsProgram.COL_RADIO]);
        if (scrobbles <= 0 && radioListens <= 0) {
          // if track somehow is marked with zero plays, report error and ignore
          reporter.incrCounter(COUNTER_KEYS.NOT_LISTEN, 1);
          return;
        }
        // if we get to here then user has listened to track, so output user id against track id
        IntWritable trackId = new IntWritable(Integer.parseInt(parts[TrackStatisticsProgram.COL_TRACKID]));
        IntWritable userId = new IntWritable(Integer.parseInt(parts[TrackStatisticsProgram.COL_USERID]));
        output.collect(trackId, userId);
      } catch (NumberFormatException e) {
        reporter.incrCounter(COUNTER_KEYS.INVALID_LINES, 1);
        reporter.setStatus("Invalid line in listening data: " + rawLine);
        return;
      }
    }
  }

  /**
   * Combiner that improves efficiency by removing duplicate user ids from mapper output.
   */
  public static class UniqueListenersCombiner extends MapReduceBase implements
      Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

    public void reduce(IntWritable trackId, Iterator<IntWritable> values,
        OutputCollector<IntWritable, IntWritable> output, Reporter reporter) throws IOException {
      
      Set<IntWritable> userIds = new HashSet<IntWritable>();
      while (values.hasNext()) {
        IntWritable userId = values.next();
        if (!userIds.contains(userId)) {
          // if this user hasn't already been marked as listening to the track, add them to set and output them
          userIds.add(new IntWritable(userId.get()));
          output.collect(trackId, userId);
        }
      }
    }
  }

  /**
   * Reducer that outputs only unique listener ids per track (i.e. it removes any duplicated). Final output is number of
   * unique listeners per track.
   */
  public static class UniqueListenersReducer extends MapReduceBase implements
      Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

    public void reduce(IntWritable trackId, Iterator<IntWritable> values,
        OutputCollector<IntWritable, IntWritable> output, Reporter reporter) throws IOException {
      
      Set<Integer> userIds = new HashSet<Integer>();
      // add all userIds to the set, duplicates automatically removed (set contract)
      while (values.hasNext()) {
        IntWritable userId = values.next();
        userIds.add(Integer.valueOf(userId.get()));
      }
      // output trackId -> number of unique listeners per track
      output.collect(trackId, new IntWritable(userIds.size()));
    }

  }

  /**
   * Mapper that summarizes various statistics per track. Input is raw listening data, output is a partially filled in
   * TrackStatistics object per track id.
   */
  public static class SumMapper extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, TrackStats> {

    public void map(LongWritable position, Text rawLine, OutputCollector<IntWritable, TrackStats> output,
        Reporter reporter) throws IOException {

      String line = (rawLine).toString();
      if (line.trim().isEmpty()) { // ignore empty lines
        reporter.incrCounter(COUNTER_KEYS.INVALID_LINES, 1);
        return;
      }

      String[] parts = line.split(" ");
      try {
        int trackId = Integer.parseInt(parts[TrackStatisticsProgram.COL_TRACKID]);
        int scrobbles = Integer.parseInt(parts[TrackStatisticsProgram.COL_SCROBBLES]);
        int radio = Integer.parseInt(parts[TrackStatisticsProgram.COL_RADIO]);
        int skip = Integer.parseInt(parts[TrackStatisticsProgram.COL_SKIP]);
        // set number of listeners to 0 (this is calculated later) and other values as provided in text file
        TrackStats trackstat = new TrackStats(0, scrobbles + radio, scrobbles, radio, skip);
        output.collect(new IntWritable(trackId), trackstat);
      } catch (NumberFormatException e) {
        reporter.incrCounter(COUNTER_KEYS.INVALID_LINES, 1);
        log.warn("Invalid line in listening data: " + rawLine);
      }
    }
  }

  /**
   * Sum up the track statistics per track. Output is a TrackStatistics object per track id.
   */
  public static class SumReducer extends MapReduceBase implements
      Reducer<IntWritable, TrackStats, IntWritable, TrackStats> {

    @Override
    public void reduce(IntWritable trackId, Iterator<TrackStats> values,
        OutputCollector<IntWritable, TrackStats> output, Reporter reporter) throws IOException {
      
      TrackStats sum = new TrackStats(); // holds the totals for this track
      while (values.hasNext()) {
        TrackStats trackStats = (TrackStats) values.next();
        sum.setListeners(sum.getListeners() + trackStats.getListeners());
        sum.setPlays(sum.getPlays() + trackStats.getPlays());
        sum.setSkips(sum.getSkips() + trackStats.getSkips());
        sum.setScrobbles(sum.getScrobbles() + trackStats.getScrobbles());
        sum.setRadioPlays(sum.getRadioPlays() + trackStats.getRadioPlays());
      }
      output.collect(trackId, sum);
    }
  }

  /**
   * Mapper that takes the number of listeners for a track and converts this to a TrackStats object which is output
   * against each track id.
   */
  public static class MergeListenersMapper extends MapReduceBase implements
      Mapper<IntWritable, IntWritable, IntWritable, TrackStats> {

    public void map(IntWritable trackId, IntWritable uniqueListenerCount,
        OutputCollector<IntWritable, TrackStats> output, Reporter reporter) throws IOException {
      
      TrackStats trackStats = new TrackStats();
      trackStats.setListeners(uniqueListenerCount.get());
      output.collect(trackId, trackStats);
    }
  }

  /**
   * Create a JobConf for a Job that will calculate the number of unique listeners per track.
   * 
   * @param inputDir The path to the folder containing the raw listening data files.
   * @return The unique listeners JobConf.
   */
  private JobConf getUniqueListenersJobConf(Path inputDir) {
    log.info("Creating configuration for unique listeners Job");

    // output results to a temporary intermediate folder, this will get deleted by start() method
    Path uniqueListenersOutput = new Path("uniqueListeners");

    JobConf conf = new JobConf(TrackStatisticsProgram.class);
    conf.setOutputKeyClass(IntWritable.class); // track id
    conf.setOutputValueClass(IntWritable.class); // number of unique listeners
    conf.setInputFormat(TextInputFormat.class); // raw listening data
    conf.setOutputFormat(SequenceFileOutputFormat.class);
    conf.setMapperClass(UniqueListenersMapper.class);
    conf.setCombinerClass(UniqueListenersCombiner.class);
    conf.setReducerClass(UniqueListenersReducer.class);

    FileInputFormat.addInputPath(conf, inputDir);
    FileOutputFormat.setOutputPath(conf, uniqueListenersOutput);
    conf.setJobName("uniqueListeners");
    return conf;
  }

  /**
   * Creates a JobConf for a Job that will sum up the TrackStatistics per track.
   * 
   * @param inputDir The path to the folder containing the raw input data files.
   * @return The sum JobConf.
   */
  private JobConf getSumJobConf(Path inputDir) {
    log.info("Creating configuration for sum job");
    // output results to a temporary intermediate folder, this will get deleted by start() method
    Path playsOutput = new Path("sum");

    JobConf conf = new JobConf(TrackStatisticsProgram.class);
    conf.setOutputKeyClass(IntWritable.class); // track id
    conf.setOutputValueClass(TrackStats.class); // statistics for a track
    conf.setInputFormat(TextInputFormat.class); // raw listening data
    conf.setOutputFormat(SequenceFileOutputFormat.class);
    conf.setMapperClass(SumMapper.class);
    conf.setCombinerClass(SumReducer.class);
    conf.setReducerClass(SumReducer.class);

    FileInputFormat.addInputPath(conf, inputDir);
    FileOutputFormat.setOutputPath(conf, playsOutput);
    conf.setJobName("sum");
    return conf;
  }

  /**
   * Creates a JobConf for a Job that will merge the unique listeners and track statistics.
   * 
   * @param outputPath The path for the results to be output to.
   * @param sumInputDir The path containing the data from the sum Job.
   * @param listenersInputDir The path containing the data from the unique listeners job.
   * @return The merge JobConf.
   */
  private JobConf getMergeConf(Path outputPath, Path sumInputDir, Path listenersInputDir) {
    log.info("Creating configuration for merge job");
    JobConf conf = new JobConf(TrackStatisticsProgram.class);
    conf.setOutputKeyClass(IntWritable.class); // track id
    conf.setOutputValueClass(TrackStats.class); // overall track statistics
    conf.setCombinerClass(SumReducer.class); // safe to re-use reducer as a combiner here
    conf.setReducerClass(SumReducer.class);
    conf.setOutputFormat(TextOutputFormat.class);

    FileOutputFormat.setOutputPath(conf, outputPath);

    MultipleInputs.addInputPath(conf, sumInputDir, SequenceFileInputFormat.class, IdentityMapper.class);
    MultipleInputs.addInputPath(conf, listenersInputDir, SequenceFileInputFormat.class, MergeListenersMapper.class);
    conf.setJobName("merge");
    return conf;
  }

  /**
   * Start the program.
   * 
   * @param inputDir The path to the folder containing the raw listening data files.
   * @param outputPath The path for the results to be output to.
   * @throws IOException If an error occurs retrieving data from the file system or an error occurs running the job.
   */
  public void start(Path inputDir, Path outputDir) throws IOException {
    FileSystem fs = FileSystem.get(this.conf);

    JobConf uniqueListenersConf = getUniqueListenersJobConf(inputDir);
    Path listenersOutputDir = FileOutputFormat.getOutputPath(uniqueListenersConf);
    Job listenersJob = new Job(uniqueListenersConf);
    // delete any output that might exist from a previous run of this job
    if (fs.exists(FileOutputFormat.getOutputPath(uniqueListenersConf))) {
      fs.delete(FileOutputFormat.getOutputPath(uniqueListenersConf), true);
    }

    JobConf sumConf = getSumJobConf(inputDir);
    Path sumOutputDir = FileOutputFormat.getOutputPath(sumConf);
    Job sumJob = new Job(sumConf);
    // delete any output that might exist from a previous run of this job
    if (fs.exists(FileOutputFormat.getOutputPath(sumConf))) {
      fs.delete(FileOutputFormat.getOutputPath(sumConf), true);
    }

    // the merge job depends on the other two jobs
    ArrayList<Job> mergeDependencies = new ArrayList<Job>();
    mergeDependencies.add(listenersJob);
    mergeDependencies.add(sumJob);
    JobConf mergeConf = getMergeConf(outputDir, sumOutputDir, listenersOutputDir);
    Job mergeJob = new Job(mergeConf, mergeDependencies);
    // delete any output that might exist from a previous run of this job
    if (fs.exists(FileOutputFormat.getOutputPath(mergeConf))) {
      fs.delete(FileOutputFormat.getOutputPath(mergeConf), true);
    }

    // store the output paths of the intermediate jobs so this can be cleaned up after a successful run
    List<Path> deletePaths = new ArrayList<Path>();
    deletePaths.add(FileOutputFormat.getOutputPath(uniqueListenersConf));
    deletePaths.add(FileOutputFormat.getOutputPath(sumConf));

    JobControl control = new JobControl("TrackStatisticsProgram");
    control.addJob(listenersJob);
    control.addJob(sumJob);
    control.addJob(mergeJob);

    // execute the jobs
    try {
      Thread jobControlThread = new Thread(control, "jobcontrol");
      jobControlThread.start();
      while (!control.allFinished()) {
        Thread.sleep(1000);
      }
      if (control.getFailedJobs().size() > 0) {
        throw new IOException("One or more jobs failed");
      }
    } catch (InterruptedException e) {
      throw new IOException("Interrupted while waiting for job control to finish", e);
    }

    // remove intermediate output paths
    for (Path deletePath : deletePaths) {
      fs.delete(deletePath, true);
    }
  }

  /**
   * Set the Configuration used by this Program.
   * 
   * @param conf The new Configuration to use by this program.
   */
  public void setConf(Configuration conf) {
    this.conf = conf; // this will usually only be set by unit test.
  }

  /**
   * Gets the Configuration used by this program.
   * 
   * @return This program's Configuration.
   */
  public Configuration getConf() {
    return conf;
  }

  /**
   * Main method used to run the TrackStatisticsProgram from the command line. This takes two parameters - first the
   * path to the folder containing the raw input data; and second the path for the data to be output to.
   * 
   * @param args Command line arguments.
   * @throws IOException If an error occurs running the program.
   */
  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      log.info("Args: <input directory> <output directory>");
      return;
    }

    Path inputPath = new Path(args[0]);
    Path outputDir = new Path(args[1]);
    log.info("Running on input directories: " + inputPath);
    TrackStatisticsProgram listeners = new TrackStatisticsProgram();
    listeners.start(inputPath, outputDir);
  }

}
