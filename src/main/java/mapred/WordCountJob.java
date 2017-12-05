package mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by beard on 2017. 12. 3..
 */
public class WordCountJob {

    public static void execute(Job job, String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
        job.setJarByClass(WordCountJob.class);
        job.setMapperClass(WordCountMapper.class);
        job.setCombinerClass(WordCountReducer.class);
        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        job.waitForCompletion(true);
    }

    public static void main(String[] args){
        try {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "mapred.WordCountJob");

            execute(job, args[0], args[1]);

            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
