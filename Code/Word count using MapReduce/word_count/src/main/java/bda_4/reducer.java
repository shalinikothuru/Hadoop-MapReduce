package bda_4;
////importing necessary libraries
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
public class reducer  extends MapReduceBase implements Reducer<Text,IntWritable,Text,IntWritable> {
    public void reduce(Text key, Iterator<IntWritable> values,OutputCollector<Text,IntWritable> output,
                       Reporter reporter) throws IOException {
        //initial count of word is set to zero
        int frequency=0;
        //iterating and combining the values of the word to get frequency of that word in total words
        while (values.hasNext())
        {
            frequency+=values.next().get();
        }
        //word wits its frequency
        output.collect(key,new IntWritable(frequency));
    }
}