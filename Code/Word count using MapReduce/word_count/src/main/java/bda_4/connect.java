package bda_4;
//importing necessary libraries
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
public class connect
{
    public static void main(String[] args) throws IOException
    {
        //configuring job
        JobConf job_conf = new JobConf(connect.class);
        job_conf.setJobName("word_count");
        job_conf.setOutputKeyClass(Text.class);
        job_conf.setOutputValueClass(IntWritable.class);
        job_conf.setMapperClass(mapper.class);
        job_conf.setReducerClass(reducer.class);
        job_conf.setInputFormat(TextInputFormat.class);
        job_conf.setOutputFormat(TextOutputFormat.class);
        //taking argument 1 as input
        FileInputFormat.setInputPaths(job_conf,new Path(args[0]));
        //taking argument 2 as output
        FileOutputFormat.setOutputPath(job_conf,new Path(args[1]));
        //running job
        JobClient.runJob(job_conf);
    }
}