import hdfs.HdfsFileReader;
import hdfs.HdfsFileWriter;
import mapred.WordCountJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.Job;

import java.io.File;
import java.net.URI;

/**
 * Created by beard on 2017. 12. 5..
 */
public class WriteReadMapReduce {
    public static void main(String[] args) {
        try {

            String inputPath = "src/main/resources/wordcount/input";
            String nameNodeUrl = "hdfs://localhost:9000";
            String destinationPath = "/wordcount/input";
            String resultPath = "/wordcount/output";
            String outputPath = "src/main/resources/wordcount/output";


            Configuration conf = new Configuration();
            FileSystem fileSystem = FileSystem.get(new URI(nameNodeUrl), conf);

            HdfsFileWriter.uploadToHdfs(fileSystem, inputPath, destinationPath);

            WordCountJob.execute(
                    Job.getInstance(conf, "mapred.WordCountJob"),
                    nameNodeUrl + File.separator + destinationPath,
                    nameNodeUrl + File.separator + resultPath);

            HdfsFileReader.downloadFromHdfs(fileSystem, resultPath, outputPath);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
