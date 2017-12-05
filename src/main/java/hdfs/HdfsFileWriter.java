package hdfs;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.Collection;

/**
 * Created by beard on 2017. 12. 4..
 */
public class HdfsFileWriter {

    public static void uploadToHdfs(FileSystem fileSystem, String targetPath, String destinationPath) throws IOException {
        // 타겟 경로 아래 파일 목록을 가져옴
        Collection<File> files = FileUtils.listFiles(new File(targetPath), new String[]{"txt"}, true);
        for (File targetFile : files) {

            // 경로 체크
            Path path = new Path(destinationPath + File.separator + targetFile.getName());
            if (fileSystem.exists(path)) {
                fileSystem.delete(path, true);
            }

            // 파일 저장
            FSDataOutputStream outputStream = fileSystem.create(path);
            IOUtils.copy(new FileInputStream(targetFile), outputStream);
            outputStream.close();

            if(fileSystem.exists(path)) {
                System.out.println(path.toString() + " upload success!");
            } else {
                System.out.println(path.toString() + " upload fail!");
            }
        }
    }

    public static void main(String[] args) {

        // 파라미터 체크
        if(args.length != 3) {
            System.err.println("Usage: HdfsFileWriter <filename> <contents>");
            System.exit(2);
        }

        try {

            String targetPath = args[0];
            String nameNodeUrl = args[1];
            String destinationPath = args[2];

            // 파일 시스템 제어 객체 생성
            Configuration conf = new Configuration();

            FileSystem fileSystem = FileSystem.get(new URI(nameNodeUrl), conf);

            uploadToHdfs(fileSystem, targetPath, destinationPath);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
