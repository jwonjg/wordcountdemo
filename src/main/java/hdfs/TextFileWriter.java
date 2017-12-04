package hdfs;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.net.URI;
import java.util.Collection;

/**
 * Created by beard on 2017. 12. 4..
 */
public class TextFileWriter {
    public static void main(String[] args) {

        // 파라미터 체크
        if(args.length != 3) {
            System.err.println("Usage: TextFileWriter <filename> <contents>");
            System.exit(2);
        }

        try {

            String targetPath = args[0];
            String nameNodeUrl = args[1];
            String destinationPath = args[2];

            // 파일 시스템 제어 객체 생성
            Configuration conf = new Configuration();

            FileSystem hdfs = FileSystem.get(new URI(nameNodeUrl), conf);

            Collection<File> files = FileUtils.listFiles(new File(targetPath), new String[]{"txt"}, true);
            for (File file : files) {

                byte[] targetFileBytes = FileUtils.readFileToByteArray(file);

                // 경로 체크
                Path path = new Path(nameNodeUrl + File.separator + destinationPath + File.separator + file.getName());
                if (hdfs.exists(path)) {
                    hdfs.delete(path, true);
                }

                // 파일 저장
                FSDataOutputStream outputStream = hdfs.create(path);
                outputStream.write(targetFileBytes);
                outputStream.close();

                System.out.println(hdfs.exists(path));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
