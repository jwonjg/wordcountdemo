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

            // 2. 스트림 생성
            // 데이터노드와 네임노드의 통신을 관리하는 DFSOutputStream 을 래핑한 클래스
            // 네임노드로부터 유효성 검사를 받음
            FSDataOutputStream outputStream = fileSystem.create(path);
            // 3. 패킷 전송
            // 내부적으로 네임노드에게 데이터노드 목록을 요청하고 데이터노드에 패킷을 전송하고 결과를 확인함
            // outputStream 의 write 메서드 호출
            IOUtils.copy(new FileInputStream(targetFile), outputStream);
            // 4. 파일 닫기
            // DFSOutputStream 에 남아 있는 모든 패킷을 파이프라인으로 프러시함 패킷 정상 저장 여부를 확인함
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

            // 1. 파일 시스템을 관리하기 위한 메서드가 정의된 추상 클래스 FileSystem 구현체는 DistributedFileSystem
            FileSystem fileSystem = FileSystem.get(new URI(nameNodeUrl), conf);

            uploadToHdfs(fileSystem, targetPath, destinationPath);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
