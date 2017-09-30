// Mithun Mistry - 800961418 - mmistry1@uncc.edu
import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


public class TermFrequency extends Configured implements Tool {

   @SuppressWarnings("unused")
private static final Logger LOG = Logger .getLogger( TermFrequency.class);

   public static void main( String[] args) throws  Exception {
      int res  = ToolRunner .run( new TermFrequency(), args);
      System .exit(res);
   }

   public int run( String[] args) throws  Exception {
      Job job  = Job .getInstance(getConf(), " termfrequency ");
      job.setJarByClass( this .getClass());

      FileInputFormat.addInputPaths(job,  args[0]);
      FileOutputFormat.setOutputPath(job,  new Path(args[ 1]));
      job.setMapperClass( Map .class);
      job.setReducerClass( Reduce .class);
      job.setOutputKeyClass( Text .class);
      job.setOutputValueClass( DoubleWritable .class);

      return job.waitForCompletion( true)  ? 0 : 1;
   }
   
   public static class Map extends Mapper<LongWritable ,  Text ,  Text ,  DoubleWritable > {
      private final static DoubleWritable one  = new DoubleWritable( 1);
      @SuppressWarnings("unused")
	private Text word  = new Text();

      private static final Pattern WORD_BOUNDARY = Pattern .compile("\\s*\\b\\s*");

      public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {

         String line  = lineText.toString();
         Text currentWordWithFile  = new Text();
         
         context.getInputSplit();
         // Getting the filename which is being processed
         String filename = ((FileSplit) context.getInputSplit()).getPath().getName();

         for ( String word  : WORD_BOUNDARY .split(line)) {
        	// Skip empty words as they are not of significance
            if (word.isEmpty()) {
               continue;
            }
         // Mapping it as Word#####FileName	1
            currentWordWithFile  = new Text(word.toLowerCase().concat("#####").concat(filename));
          //Output of map with Word#####FileName as key and 1 as value
            context.write(currentWordWithFile,one);
         }
      }
   }

   public static class Reduce extends Reducer<Text ,  DoubleWritable ,  Text ,  DoubleWritable > {
      @Override 
      public void reduce( Text word,  Iterable<DoubleWritable > counts,  Context context)
         throws IOException,  InterruptedException {
         double sum  = 0;
         // We will saved word frequency in this double
         double word_frequency = 0;
         for ( DoubleWritable count  : counts) {
            sum  += count.get();
            // Word Frequency formula
            word_frequency = 1 + Math.log10(sum);
         }
       //Output will be Word as Key and Word_frequency as value
         context.write(word,  new DoubleWritable(word_frequency));
      }
   }
}
