// Mithun Mistry - 800961418 - mmistry1@uncc.edu
import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


public class Search extends Configured implements Tool {

   @SuppressWarnings("unused")
private static final Logger LOG = Logger .getLogger( Search.class);

   public static void main( String[] args) throws  Exception {
      int res  = ToolRunner .run( new Search(), args);
      System .exit(res);
   }

   public int run( String[] args) throws  Exception {
      Job job  = Job .getInstance(getConf(), "search");
      job.setJarByClass( this .getClass());

      FileInputFormat.addInputPaths(job,  args[0]);
      FileOutputFormat.setOutputPath(job,  new Path(args[1]));
      
      Configuration configuration = job.getConfiguration();
      
      configuration.set("Query", args[2]);
      
      job.setMapperClass( Map .class);
      job.setReducerClass( Reduce .class);
      
      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(DoubleWritable.class);
      

      return job.waitForCompletion( true)  ? 0 : 1;
   }
   
   public static class Map extends Mapper<LongWritable ,  Text ,  Text ,  DoubleWritable > {

      public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {
    	  // Splitting query so we get an array so we can check current_word processed by mapper is present in the query or not
    	 String[] query = context.getConfiguration().get("Query").toLowerCase().split("\\s+");
    	 // Splitting current line i.e. line[0] is key of previous output and line[1] is value
    	 String[] line = lineText.toString().split("\\s+");
    	 // Splitting current_word and filename
         String[] intermediate  = line[0].toString().split("#####");
         // Check if current word is present in query
         if (Arrays.asList(query).contains(intermediate[0])){
        	 // intermediate[0] will be the word and intermediate[1] is the file name, line[1] is tfidf value
        	 context.write(new Text(intermediate[1]), new DoubleWritable(Double.parseDouble(line[1])));
         }
      }
   }

   public static class Reduce extends Reducer<Text ,  DoubleWritable ,  Text ,  DoubleWritable > {
      @Override 
      public void reduce( Text document_name,  Iterable<DoubleWritable > tfidf,  Context context)
         throws IOException,  InterruptedException {
         // We will saved word frequency in this double
         double tfidf_sum = 0;
         for ( DoubleWritable count  : tfidf) {
        	 // calculating sum of tfidf
             tfidf_sum  += count.get();
         }
         // give user the "filename	TFIDF_Score"	
         context.write(document_name,  new DoubleWritable(tfidf_sum));
      }
   }
}
