package ca.uwaterloo.cs.bigdata2016w.evisoup.assignment1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;
import java.util.HashSet;
import java.util.HashMap;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;

import tl.lin.data.pair.PairOfStrings;
import tl.lin.data.map.HMapStFW;
import tl.lin.data.map.HMapStFW;



import java.io.*;
import java.util.*;
import java.net.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;





/**
 * Simple word count demo.
 */
public class StripesPMI extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(StripesPMI.class);

  // Mapper: emits (token, 1) for every word occurrence.
  private static class MyMapper extends Mapper<LongWritable, Text, Text, HMapStFW>  {
    // Reuse objects to save overhead of object creation.
    private static final HMapStFW MAP = new HMapStFW();
    private static final Text KEY = new Text();

   @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      String line = ((Text) value).toString();
      

      int cnt = 0;
      //Set set = Sets.newHashSet();
      HashSet<String> set = new HashSet<String>();

      StringTokenizer itr = new StringTokenizer(line);
      while (itr.hasMoreTokens()) {
        cnt++;
        String w = itr.nextToken().toLowerCase().replaceAll("(^[^a-z]+|[^a-z]+$)", "");
        if (w.length() == 0) continue;
        set.add(w);
        if (cnt >= 100) break;
      }
      
      String[] words = new String[set.size()];
      words = set.toArray(words);

      // Your code goes here...
     
      for (int i = 0; i < words.length; ++i ) {
        MAP.clear();

        String left = words[i];

        for (int j = 0 ; j < words.length ; j++) {

          if( i == j){
            continue;
          }
          else
          {
            String right = words[j];
            MAP.increment( right );
          }

        }
        KEY.set(left);
        context.write(KEY, MAP);

      }


      //////////
   }
   
  }
  
  
  protected static class MyCombiner extends Reducer<Text, HMapStFW, Text, HMapStFW> {
    @Override
    public void reduce(Text key, Iterable<HMapStFW> values, Context context)
        throws IOException, InterruptedException {
      Iterator<HMapStFW> iter = values.iterator();
      HMapStFW map = new HMapStFW();

      while (iter.hasNext()) {
        map.plus(iter.next());
      }

      context.write(key, map);
    }
  }





//Reducer: sums up all the counts.(suming)
protected static class MyReducer extends Reducer<Text, HMapStFW, Text, HMapStFW> {
    

    private HashMap<String, Integer> sidedata = new HashMap<String, Integer>();

    @Override
    protected void setup(Context context) throws IOException{

      try{

        FileSystem fs = FileSystem.get(new Configuration());
                        //FileStatus[] status = fs.listStatus(new Path("hdfs://jp.seka.com:9000/user/jeka/in"));
        FileStatus[] status = fs.listStatus(new Path("cs489-2016w-evisoup-a1-shakespeare-line/part-r-00000"));
        for (int i=0;i<status.length;i++){
                BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
                String line;
                line=br.readLine();

                StringTokenizer itr; 
                  
                String word;// = itr.nextToken();
                String count; //= itr.nextToken();
                int c; //= Integer.parseInt(count);
                // sidedata.put(word, c);

                //System.out.println( ">>>>>>>>>>>>>>2<<<<<<<<<<<<<" );
                while (line != null){                    
                                      //System.out.println( ">>>>>>>>>>>>>>3<<<<<<<<<<<<<" );
                                      
                        itr = new StringTokenizer(line);
                                      //System.out.println( ">>>>>>>>>>>>>>4<<<<<<<<<<<<<" );
                        word = itr.nextToken();
                                      //System.out.println( ">>>>>>>>>>>>>>5<<<<<<<<<<<<<" );
                        count = itr.nextToken();
                                      //System.out.println( ">>>>>>>>>>>>>>6<<<<<<<<<<<<<" );
                        c = Integer.parseInt(count);
                                      //System.out.println( ">>>>>>>>>>>>>>7<<<<<<<<<<<<<" );

                        sidedata.put(word, c);
                                      //System.out.println( ">>>>>>>>>>>>>>7.5<<<<<<<<<<<<<" );
                        line=br.readLine();
                                      //System.out.println( ">>>>>>>>>>>>>>8<<<<<<<<<<<<<" );
                }
                //System.out.println( ">>>>>>>>>>>>>>9<<<<<<<<<<<<<" );

                //int gg =  sidedata.get("doublet");

                // System.out.println( ">>>>>>>>>>>>>>10<<<<<<<<<<<<<" );
                // System.out.println( gg );
                


        }

      }catch( Exception e){
        System.out.println("input file not found!!!!!!!!!!!!!!!");
      }

      //  /u6/h225liu/cs489/bigdata2016w/sidedata/part-r-00000

    }
    //Done setup, sidedata(HashMap available)
    //////////



    /////HERE !
    @Override
    public void reduce(Text key, Iterable<HMapStFW> values, Context context)
        throws IOException, InterruptedException {
      

      HMapStFW finalMap = new HMapStFW();

      Iterator<HMapStFW> iter = values.iterator();
      HMapStFW map = new HMapStFW();

      while (iter.hasNext()) {
        map.plus(iter.next());
      }

      Iterator<String> itr = map.keySet().iterator();

      while( itr.hasNext() ){
        
        String right = itr.next();
        
        if(map.get(right) > 9 ){

          float total = (float)sidedata.get("TotalLine");

          String x = key.toString() ; // directly from input
          String y = right;  //from key set
          // System.out.println( ">>>>>>>>>>>>>>left: " );
          // System.out.println( x );
          // System.out.println( "<<<<<<<<<<<<<<<<<< " );
          // System.out.println( ">>>>>>>>>>>>>>right: " );
          // System.out.println( y );
          // System.out.println( "<<<<<<<<<<<<<<<<<< " );

          // LOG.info(" >>>>>>>>>>>>>>left: " + x );
          // LOG.info(" >>>>>>>>>>>>>>right: " + y );

          float n_x = (float)sidedata.get(x);

          float n_y = (float)sidedata.get(y);

          float sum = (float)map.get(right);

          float result =(float)Math.log10( total * sum / (n_x * n_y) );

          finalMap.put(y, result);

          //context.write(key, )
          
          }
      }

      if(finalMap.size() > 0) {
        context.write(key, finalMap);
      }




    }


    //   if(sum >= 10.0f){
    //     float total = (float)sidedata.get("TotalLine");


    //     String x = key.getLeftElement();
    //     String y = key.getRightElement();

    //     float n_x = (float)sidedata.get(x);
    //     //float p_x = x/total;

    //     float n_y = (float)sidedata.get(y);
    //     //float p_y = y/total;

    //     float n_x_y = (n_x + n_y);

    //     float result =(float)Math.log10( total * sum / (n_x * n_y) );

    //     VALUE.set(result);
    //   context.write(key, VALUE);


    //   }
      
                   
    // }

    }



  /**
   * Creates an instance of this tool.
   */
  private StripesPMI() {}

  public static class Args {
    @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
    public String input;

    @Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
    public String output;

    @Option(name = "-reducers", metaVar = "[num]", required = false, usage = "number of reducers")
    public int numReducers = 1;

    @Option(name = "-textOutput", required = false, usage = "use TextOutputFormat (otherwise, SequenceFileOutputFormat)")
    public boolean textOutput = false;
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

    LOG.info("Tool name: " + StripesPMI.class.getSimpleName());
    LOG.info(" - input path: " + args.input);
    LOG.info(" - output path: " + args.output);
    LOG.info(" - num reducers: " + args.numReducers);
    LOG.info(" - text output: " + args.textOutput);

    Job job = Job.getInstance(getConf());
    job.setJobName(StripesPMI.class.getSimpleName());
    job.setJarByClass(StripesPMI.class);

    job.setNumReduceTasks(args.numReducers);

    //job.addCacheFile(new URI("sidedata/part-r-00000"));

    FileInputFormat.setInputPaths(job, new Path(args.input));
    FileOutputFormat.setOutputPath(job, new Path(args.output));

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(HMapStFW.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(HMapStFW.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    /*
    if (args.textOutput) {
      job.setOutputFormatClass(TextOutputFormat.class);
    } else {
      job.setOutputFormatClass(SequenceFileOutputFormat.class);
    }*/

    job.setMapperClass(MyMapper.class);
    job.setCombinerClass(MyCombiner.class);
    job.setReducerClass(MyReducer.class);
    //job.setPartitionerClass(MyPartitioner.class);

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
    ToolRunner.run(new StripesPMI(), args);
  }
  //end of class
}