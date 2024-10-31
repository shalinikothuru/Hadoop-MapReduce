package bda_4;
import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.JobConf;
public class reducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
    private int N;
    //TreeMap to store word and its frequency
    private TreeMap<Integer, Text> top_N_words;
    private OutputCollector<Text, IntWritable> result;
    public void configure(JobConf job)
    {
        //getting n value from configuration, I gave default value as 3
        N = job.getInt("top_N", 3);
        // new Treemap for the tokens
        top_N_words = new TreeMap<>(Comparator.reverseOrder());
    }

    public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output,
                       Reporter reporter) throws IOException {
        int frequency = 0;
        while (values.hasNext()) {
            frequency += values.next().get();
        }
        //adding word and its frequency to the TreeMap
        top_N_words.put(frequency, new Text(key.toString()));
        //keeping only top N words
        if (top_N_words.size() > N)
        {
            top_N_words.pollLastEntry();
        }
        //storing the OutputCollector for later use in the close method as I can't use output directly in close method
        result = output;
    }
    public void close() throws IOException
    {
        //emitting the top N words in descending order
        for (Map.Entry<Integer, Text> entry : top_N_words.entrySet())
        {
            result.collect(entry.getValue(), new IntWritable(entry.getKey()));
        }
    }
}
