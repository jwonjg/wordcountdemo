package hdfs;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.*;
import java.net.URI;

/**
 * Created by beard on 2017. 12. 5..
 */
public class HdfsFileReader {

    public static void downloadFromHdfs(FileSystem fileSystem, String targetPath, String destinationPath) throws IOException {

        Path path = new Path(targetPath);
        if(fileSystem.exists(path)) {
            // HDFS 다운로드 대상 디렉토리 경로 체크
            RemoteIterator<LocatedFileStatus> remoteIterator = fileSystem.listFiles(path, true);
            while (remoteIterator.hasNext()) {

                // 다운로드 대상 파일 경로 체크
                LocatedFileStatus fileStatus = remoteIterator.next();
                Path filePath = fileStatus.getPath();

                // 다운로드 받을 경로
                File destinationFile = new File(destinationPath + File.separator + filePath.getName());

                // 파일 다운로드
                IOUtils.copy(fileSystem.open(filePath), new FileOutputStream(destinationFile));

                if(destinationFile.exists()) {
                    System.out.println(path.toString() + " download success!");
                } else {
                    System.out.println(path.toString() + " download fail!");
                }
                System.out.println(filePath);
            }

        } else {
            System.err.println("Invalid Target Directory");
            System.exit(2);
        }
    }

    public static void main(String[] args) {

        // 파라미터 체크
        if(args.length != 3) {
            System.err.println("Usage: HdfsFileReader <filename> <contents>");
            System.exit(2);
        }

        try {

            String nameNodeUrl = args[0];
            String targetPath = args[1];
            String destinationPath = args[2];

            // 파일 시스템 제어 객체 생성
            Configuration conf = new Configuration();

            FileSystem fileSystem = FileSystem.get(new URI(nameNodeUrl), conf);

            downloadFromHdfs(fileSystem, targetPath, destinationPath);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
