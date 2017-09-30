// Mithun Mistry - 800961418 - mmistry1@uncc.edu
import java.io.IOException;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


public class TFIDF extends Configured implements Tool {

   @SuppressWarnings("unused")
private static final Logger LOG = Logger .getLogger( TFIDF.class);

   public static void main( String[] args) throws  Exception {
      int res  = ToolRunner .run( new TermFrequency(), args);
      //chaining map reduce operations
	  int res_tfidf = ToolRunner.run(new TFIDF(), args);
      System .exit(res);
   }

   public int run( String[] args) throws  Exception {
      Job job  = Job .getInstance(getConf(), " tfidf ");
      job.setJarByClass( this .getClass());
      
      Configuration configuration = job.getConfiguration();
      
	  // getting total number of documents
      FileSystem filesystem = FileSystem.get(configuration);
	  Path path = new Path(args[0]);
	  ContentSummary contentsummary = filesystem.getContentSummary(path);
	  long fileCount = contentsummary.getFileCount();
	
	  configuration.setInt("TotalDocuments", (int) fileCount);

      FileInputFormat.addInputPaths(job,  args[1]);
      FileOutputFormat.setOutputPath(job,  new Path(args[2]));
      job.setJarByClass(this.getClass());
      job.setMapperClass( Map .class);
      job.setReducerClass( Reduce .class);
      job.setOutputKeyClass( Text .class);
      job.setOutputValueClass( DoubleWritable .class);
      
      // Stating mapper and reducer's input and output class
      job.setOutputKeyClass(Text.class);
	  job.setOutputValueClass(DoubleWritable.class);
	  job.setMapOutputKeyClass(Text.class);
	  job.setMapOutputValueClass(Text.class);

      return job.waitForCompletion( true)  ? 0 : 1;
   }
   
   public static class Map extends Mapper<LongWritable,  Text,  Text ,Text > {

      public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {
    	  
    	  	Text current_word = new Text();
			String[] line_split = lineText.toString().split("#####");
			current_word = new Text(line_split[0]);

			String value = line_split[1].replaceAll("\\s+", "=");

			context.write(current_word, new Text(value));
      }
   }

   public static class Reduce extends Reducer<Text ,  Text,  Text ,  DoubleWritable > {
      @Override 
      public void reduce( Text word,  Iterable<Text> fname_wf,  Context context)
         throws IOException,  InterruptedException {
    	  
    	  HashMap<String, String> hashMap = new HashMap<>();
    	  int DocumentFrequency = 0;
		  for (Text count : fname_wf) {
			String[] intermediate = count.toString().split("=");
			hashMap.put(intermediate[0], intermediate[1]);
			DocumentFrequency++;
		  }
		  for (String key : hashMap.keySet()) {
			  double TermFrequency = Double.parseDouble(hashMap.get(key));
			  double IDF = Math.log10(1.0 + (Double.valueOf(context.getConfiguration().get("TotalDocuments")) / Double.valueOf(DocumentFrequency)));
			  double tf_idf = TermFrequency * IDF;
			  String ReducerOutputKey = word.toString() + "#####" + key;
			
			  context.write(new Text(ReducerOutputKey), new DoubleWritable(tf_idf));
		}
      }
   }
}
