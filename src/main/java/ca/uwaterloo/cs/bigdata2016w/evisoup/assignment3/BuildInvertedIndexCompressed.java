package ca.uwaterloo.cs.bigdata2016w.evisoup.assignment3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;

import tl.lin.data.array.ArrayListWritable;
import tl.lin.data.fd.Object2IntFrequencyDistribution;
import tl.lin.data.fd.Object2IntFrequencyDistributionEntry;
import tl.lin.data.pair.PairOfInts;
import tl.lin.data.pair.PairOfObjectInt;
import tl.lin.data.pair.PairOfWritables;
import tl.lin.data.pair.PairOfStringInt;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.WritableUtils;



import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;



public class BuildInvertedIndexCompressed extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(BuildInvertedIndexCompressed.class);

  private static class MyMapper extends Mapper<LongWritable, Text, PairOfStringInt, VIntWritable> {
    //private static final Text WORD = new Text();
    private static final PairOfStringInt KEYPAIR = new PairOfStringInt();
    private static final VIntWritable VALUE = new VIntWritable();
    private static final Object2IntFrequencyDistribution<String> COUNTS =
        new Object2IntFrequencyDistributionEntry<String>();

    @Override
    public void map(LongWritable docno, Text doc, Context context)
        throws IOException, InterruptedException {
      String text = doc.toString();

      // Tokenize line.
      List<String> tokens = new ArrayList<String>();
      StringTokenizer itr = new StringTokenizer(text);
      while (itr.hasMoreTokens()) {
        String w = itr.nextToken().toLowerCase().replaceAll("(^[^a-z]+|[^a-z]+$)", "");
        if (w.length() == 0) continue;
        tokens.add(w);
      }

      // Build a histogram of the terms.
      COUNTS.clear();
      for (String token : tokens) {
        COUNTS.increment(token);
      }

      // Emit postings.
      for (PairOfObjectInt<String> e : COUNTS) {

        KEYPAIR.set(e.getLeftElement(), (int)docno.get());
        VALUE.set(e.getRightElement());
        context.write(KEYPAIR,  VALUE);

      }
    }
  }

  private static class MyReducer extends
      Reducer<PairOfStringInt, VIntWritable, Text, BytesWritable> {
    
    private static  BytesWritable POSTINGS ;
    private  static Text tPrev ; 
    private  static ByteArrayOutputStream byteOutput ; 
    private  static DataOutputStream dataOutput ; 
    private  static IntWritable PREDOCID ; 



    @Override
    public void setup(Context context) {

      POSTINGS = new BytesWritable();
      tPrev = new Text();
      tPrev.set("");
      byteOutput = new ByteArrayOutputStream();
      dataOutput = new DataOutputStream(byteOutput);
      PREDOCID = new IntWritable();
      PREDOCID.set(0);

    }

    @Override
    public void reduce(PairOfStringInt key, Iterable<VIntWritable> values, Context context)
        throws IOException, InterruptedException {


      Iterator<VIntWritable> iter = values.iterator();
      int tf = 0;


      if( !key.getLeftElement().equals( tPrev.toString()  )  && !tPrev.toString().equals("")  ){

        POSTINGS.set(byteOutput.toByteArray(), 0, byteOutput.size() ) ;
        context.write(tPrev, POSTINGS);
       
        PREDOCID.set(0);
        dataOutput.flush();
        byteOutput.reset();

        //POSTINGS.setSize(0);
      
      }
      

      if(iter.hasNext()) {

        tf = iter.next().get();
      }

      WritableUtils.writeVInt(dataOutput, (key.getRightElement()- (int)PREDOCID.get() ));
      WritableUtils.writeVInt(dataOutput, tf);
      
      tPrev.set( key.getLeftElement() );  
      PREDOCID.set( key.getRightElement() );

      
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        context.write( tPrev , POSTINGS);
    }

  }


  protected static class MyPartitioner extends Partitioner<PairOfStringInt, VIntWritable> {
    @Override
    public int getPartition(PairOfStringInt key, VIntWritable value, int numReduceTasks) {
      return (key.getLeftElement().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
  }




  private BuildInvertedIndexCompressed() {}

  public static class Args {
    @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
    public String input;

    @Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
    public String output;

    @Option(name = "-reducers", metaVar = "[num]", required = false, usage = "number of reducers")
    public int numReducers = 1;


  }

  /**
   * Runs this tool.
   */
  public int run(String[] argv) throws Exception {
    Args args = new Args();
    CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(100));

    try {
      parser.parseArgument(argv);
    } catch (CmdLineException e) {
      System.err.println(e.getMessage());
      parser.printUsage(System.err);
      return -1;
    }

    LOG.info("Tool: " + BuildInvertedIndexCompressed.class.getSimpleName());
    LOG.info(" - input path: " + args.input);
    LOG.info(" - output path: " + args.output);
    LOG.info(" - num reducers: " + args.numReducers);

    Job job = Job.getInstance(getConf());
    job.setJobName(BuildInvertedIndexCompressed.class.getSimpleName());
    job.setJarByClass(BuildInvertedIndexCompressed.class);

    job.setNumReduceTasks(args.numReducers);

    FileInputFormat.setInputPaths(job, new Path(args.input));
    FileOutputFormat.setOutputPath(job, new Path(args.output));

    job.setMapOutputKeyClass(PairOfStringInt.class);
    job.setMapOutputValueClass(VIntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(BytesWritable.class);
    job.setOutputFormatClass(MapFileOutputFormat.class);

    job.setMapperClass(MyMapper.class);
    job.setReducerClass(MyReducer.class);
    job.setPartitionerClass(MyPartitioner.class);

    // Delete the output directory if it exists already.
    Path outputDir = new Path(args.output);
    FileSystem.get(getConf()).delete(outputDir, true);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new BuildInvertedIndexCompressed(), args);
  }
}
