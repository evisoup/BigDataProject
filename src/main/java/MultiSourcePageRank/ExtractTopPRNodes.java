 package ca.uwaterloo.cs.evisoup.assignment4;

import java.io.*;
import java.util.Arrays;
import java.util.Iterator;
import java.util.*;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import tl.lin.data.pair.PairOfObjectFloat;
import tl.lin.data.pair.*;
import tl.lin.data.queue.TopScoredObjects;

import org.apache.hadoop.fs.FSDataInputStream;

public class ExtractTopPRNodes extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(ExtractTopPRNodes.class);

  private static class MyMapper extends
      Mapper<IntWritable, PageRankNode, PairOfInts, FloatWritable> {
    private ArrayList<TopScoredObjects<Integer>> queue;
    private static String inputSource;
    private static String modSrouce[]; 

    private static int sourceNum;


    @Override
    public void setup(Context context) throws IOException {
      int k = context.getConfiguration().getInt("n", 100);
      //queue = new TopScoredObjects<Integer>(k);
      inputSource =context.getConfiguration().get(SOURCES);
      modSrouce = inputSource.split(",");
      sourceNum = modSrouce.length;

      queue = new ArrayList<TopScoredObjects<Integer>>();

      for(int i = 0; i < sourceNum; i ++){

        queue.add( new TopScoredObjects<Integer>(k) );

      }



    }

    @Override
    public void map(IntWritable nid, PageRankNode node, Context context) throws IOException,
        InterruptedException {
          for(int i = 0; i < sourceNum; i ++){
            queue.get(i).add( node.getNodeId(),  node.getPageRank().get(i) );
          }
      // queue.add(node.getNodeId(), node.getPageRank());
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
      PairOfInts key = new PairOfInts();
      FloatWritable value = new FloatWritable();

      for(int i = 0; i < sourceNum; i ++){
        for (PairOfObjectFloat<Integer> pair : queue.get(i).extractAll()) {
          key.set(pair.getLeftElement(), i);
          //(nodeID, souceNum)
          value.set(pair.getRightElement());
          //(pageRank)
          context.write(key, value);
        }
      }
    }
  }

  private static class MyReducer extends
      Reducer<PairOfInts, FloatWritable, Text, Text> {
    private ArrayList<TopScoredObjects<Integer>> queue;
    private static String inputSource;
    private static String modSrouce[]; 

    private static int sourceNum;
        Text one; 
    Text two; 


    @Override
    public void setup(Context context) throws IOException {
      int k = context.getConfiguration().getInt("n", 100);
      
      inputSource =context.getConfiguration().get(SOURCES);
      modSrouce = inputSource.split(",");
      sourceNum = modSrouce.length;
            one = new Text(); 
      two = new Text(); 
      queue = new ArrayList<TopScoredObjects<Integer>>();

      for(int i = 0; i < sourceNum; i ++){

        queue.add( new TopScoredObjects<Integer>(k) );

      }
    }

    @Override
    public void reduce(PairOfInts nid, Iterable<FloatWritable> iterable, Context context)
        throws IOException {
      Iterator<FloatWritable> iter = iterable.iterator();
      

      while(iter.hasNext() ){

        queue.get(nid.getRightElement()).add(nid.getLeftElement(), iter.next().get());
      }
      //nid = (nodeID, souceNum)
      //iterable = (pageRank)
      //add =  (nodeID, pagerank)

      // Shouldn't happen. Throw an exception.
      if (iter.hasNext()) {
        throw new RuntimeException();
      }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
      IntWritable key = new IntWritable();
      FloatWritable value = new FloatWritable();


    // private static String inputSource;
    // private static String modSrouce[]; 
    // private static int sourceNum;

      //nid = (nodeID, souceNum)
      //iterable = (pageRank)
      //add =  (nodeID, pagerank)


      for(int i = 0; i < sourceNum; i ++){
        System.out.println("source: " + modSrouce[i]);

        
        one.set("source: " );
        two.set(modSrouce[i]);
        context.write(one, two );

        for (PairOfObjectFloat<Integer> pair : queue.get(i).extractAll()) {
          key.set(pair.getLeftElement());
          value.set((float) Math.exp(pair.getRightElement()));
          String left;
          String right;
          left = String.format("%.5f", (float)value.get()  );
          right = Integer.toString(key.get());
          
          one.set(left);
          two.set(right);
          context.write(one, two );

          System.out.println(left + " " +right);
        }   

        one.set(" " );
        two.set(" ");
        context.write(one, two );
        System.out.println(" " );


      }

     /// 
    }
  }

  public ExtractTopPRNodes() {
  }

  private static final String INPUT = "input";
  private static final String OUTPUT = "output";
  private static final String TOP = "top";
  private static final String SOURCES = "sources";

  /**
   * Runs this tool.
   */
  @SuppressWarnings({ "static-access" })
  public int run(String[] args) throws Exception {
    Options options = new Options();

    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("input path").create(INPUT));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("output path").create(OUTPUT));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("top n").create(TOP));
    options.addOption(OptionBuilder.withArgName("sources").hasArg()
        .withDescription("sources").create(SOURCES));

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();

    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    if (!cmdline.hasOption(SOURCES) || !cmdline.hasOption(INPUT) || !cmdline.hasOption(OUTPUT) || !cmdline.hasOption(TOP)) {
      System.out.println("args: " + Arrays.toString(args));
      HelpFormatter formatter = new HelpFormatter();
      formatter.setWidth(120);
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    String inputPath = cmdline.getOptionValue(INPUT);
    String outputPath = cmdline.getOptionValue(OUTPUT);
    int n = Integer.parseInt(cmdline.getOptionValue(TOP));
    String s = cmdline.getOptionValue(SOURCES);
    //String sources = s.split(",");

    LOG.info("Tool name: " + ExtractTopPRNodes.class.getSimpleName());
    LOG.info(" - input: " + inputPath);
    LOG.info(" - output: " + outputPath);
    LOG.info(" - top: " + n);
    LOG.info(" - sources: " + s);

    Configuration conf = getConf();
    conf.setInt("mapred.min.split.size", 1024 * 1024 * 1024);
    conf.setInt("n", n);
    conf.setStrings(SOURCES, s);

    Job job = Job.getInstance(conf);
    job.setJobName(ExtractTopPRNodes.class.getName() + ":" + inputPath);
    job.setJarByClass(ExtractTopPRNodes.class);

    job.setNumReduceTasks(1);

    FileInputFormat.addInputPath(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));

    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setMapOutputKeyClass(PairOfInts.class);
    job.setMapOutputValueClass(FloatWritable.class);

    // job.setOutputKeyClass(FloatWritable.class);
    // job.setOutputValueClass(IntWritable.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);


    job.setMapperClass(MyMapper.class);
    job.setReducerClass(MyReducer.class);

    // Delete the output directory if it exists already.
    FileSystem.get(conf).delete(new Path(outputPath), true);

    job.waitForCompletion(true);

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new ExtractTopPRNodes(), args);
    System.exit(res);
  }
}



