

package ca.uwaterloo.cs.bigdata2016w.evisoup.assignment7;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.EOFException;
import java.io.InputStreamReader;
import java.io.*;
import java.util.Set;
import java.util.Stack;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
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
import java.io.DataInputStream;
import java.io.ByteArrayInputStream;
import java.util.Arrays;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Get;
import java.util.Map;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import java.util.Map.Entry;
import java.util.NavigableMap;



public class BooleanRetrievalHBase extends Configured implements Tool {


  private FSDataInputStream collection;
  private Stack<Set<Integer>> stack;
  private HTableInterface table;

  //private int part;
  private FileSystem fs;

  private BooleanRetrievalHBase() {}

  private void initialize(String collectionPath, FileSystem fs ) throws IOException {

    collection = fs.open(new Path(collectionPath));
    stack = new Stack<Set<Integer>>();
  
  }

  private void runQuery(String q) throws IOException {
    String[] terms = q.split("\\s+");

    for (String t : terms) {
      if (t.equals("AND")) {
        performAND();
      } else if (t.equals("OR")) {
        performOR();
      } else {
        pushTerm(t);
      }
    }

    Set<Integer> set = stack.pop();

    for (Integer i : set) {
      String line = fetchLine(i);
      System.out.println(i + "\t" + line);
    }
  }

  private void pushTerm(String term) throws IOException {
    stack.push(fetchDocumentSet(term));
  }

  private void performAND() {
    Set<Integer> s1 = stack.pop();
    Set<Integer> s2 = stack.pop();

    Set<Integer> sn = new TreeSet<Integer>();

    for (int n : s1) {
      if (s2.contains(n)) {
        sn.add(n);
      }
    }

    stack.push(sn);
  }

  private void performOR() {
    Set<Integer> s1 = stack.pop();
    Set<Integer> s2 = stack.pop();

    Set<Integer> sn = new TreeSet<Integer>();

    for (int n : s1) {
      sn.add(n);
    }

    for (int n : s2) {
      sn.add(n);
    }

    stack.push(sn);
  }

  private Set<Integer> fetchDocumentSet(String term) throws IOException {
    Set<Integer> set = new TreeSet<Integer>();

    try {

      Result posting = fetchPostings(term);
      //Map<byte[],byte[]> famMap = posting.getFamilyMap( BuildInvertedIndexHBase.CF );
      
      NavigableMap<byte[], byte[]> postings = result.getFamilyMap(  BuildInvertedIndexHBase.CF );
      //Map<byte[],byte[]> famMap = posting.getFamilyMap(Bytes.toBytes("p"));
      for (Entry<byte[],byte[]> entry : famMap.entrySet()) {
          
          set.add(Bytes.toInt(entry.getKey()));
      }

    }catch (Exception e) {}
    

    return set;
  }

  /////////////////////////////////////////////////////////////////////////////////////
  
  private Result fetchPostings(String term) throws IOException {
    
    // Text key = new Text();
    // BytesWritable val = new BytesWritable();
    // ArrayListWritable<PairOfInts> posting = new ArrayListWritable<PairOfInts>();
    // int part = (term.hashCode() & Integer.MAX_VALUE) % numReducer;
    // String temp = "/part-r-0000" + Integer.toString(part);
    // index = new MapFile.Reader(new Path(indexPath + temp ), fs.getConf());
    // key.set(term);
    // index.get(key, val);
    // ByteArrayInputStream byteInput = new ByteArrayInputStream(val.getBytes() );
    // DataInputStream dataInput = new DataInputStream(byteInput);
    // try{
    //   if( dataInput.available() != 0 ){
    //     int left = WritableUtils.readVInt(dataInput);
    //     int right = WritableUtils.readVInt(dataInput);
    //     int docID= 0;
    //       while( dataInput.available() != 0 ){

    //         docID += left ;
    //         posting.add( new PairOfInts(docID,right) );
    //         left = WritableUtils.readVInt(dataInput);
    //         right = WritableUtils.readVInt(dataInput);
    //       }
    //   }else {
    //     //System.out.println(">>>>>>FIRST OUT<<<<<<<<");
    //   }
    // }catch (Exception e){}
    
    // return posting;

    Get get = new Get(Bytes.toBytes(term));
    Result result = table.get(get);

    return result;
  }
  ////////////////////////////////////////

  public String fetchLine(long offset) throws IOException {
    collection.seek(offset);
    BufferedReader reader = new BufferedReader(new InputStreamReader(collection));

    String d = reader.readLine();
    return d.length() > 80 ? d.substring(0, 80) + "..." : d;
  }

  public static class Args {


    @Option(name = "-collection", metaVar = "[path]", required = true, usage = "collection path")
    public String collection;

    @Option(name = "-query", metaVar = "[term]", required = true, usage = "query")
    public String query;

     @Option(name = "-table", metaVar = "[name]", required = true, usage = "HBase table")
    public String table;

    @Option(name = "-config", metaVar = "[path]", required = true, usage = "HBase config")
    public String config;

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

    if (args.collection.endsWith(".gz")) {
      System.out.println("gzipped collection is not seekable: use compressed version!");
      return -1;
    }
    ///////
    Configuration conf = getConf();
    conf.addResource(new Path(args.config));

    Configuration hbaseConfig = HBaseConfiguration.create(conf);
    HConnection hbaseConnection = HConnectionManager.createConnection(hbaseConfig);
    table = hbaseConnection.getTable(args.table);
    //////////

  FileSystem fs = FileSystem.get(new Configuration());
   initialize( args.collection, fs);



    ////////////////////////
    System.out.println("Query: " + args.query);
    long startTime = System.currentTimeMillis();
    runQuery(args.query);
    System.out.println("\nquery completed in " + (System.currentTimeMillis() - startTime) + "ms");

    return 1;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new BooleanRetrievalHBase(), args);
  }
}
