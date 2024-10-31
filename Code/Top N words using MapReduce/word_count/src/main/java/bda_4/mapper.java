package bda_4;
//importing necessary libraries
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
public class mapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text token = new Text();
    private ObjectMapper map_object = new ObjectMapper();
    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output,
                    Reporter reporter) throws IOException {
        String line = value.toString();
        try
        {
            //parsing the json data
            JsonNode json_node = map_object.readTree(line);
            //extracting the "reviewText" attribute from JSON
            String reviewText = json_node.get("reviewText").asText();
            //removing punctuation and converting words to lowercase
            reviewText = reviewText.replaceAll("[^a-zA-Z\\s]", "").toLowerCase();
            //splitting the processed sentences into words
            String[] words = reviewText.split("\\s+");
            //emitting each word with 1 as its count
            for (String word : words)
            {
                this.token.set(word);
                output.collect(this.token, one);
            }
        }
        //exception handling
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }
}
