package spark.springboot;

import org.apache.hadoop.fs.FileStatus;
import org.junit.After;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.hadoop.fs.FsShell;

/**
 * @ClassName SpringBootHDFSApp
 * @Description TODO
 * @Author Spark
 * @Date 8/16/2019 4:14 PM
 * @Version 1.0
 **/
@SpringBootApplication
public class SpringBootHDFSApp implements CommandLineRunner {

    @Autowired
    FsShell fsShell;

    @Override
    public void run(String... strings) throws Exception {
        for(FileStatus fileStatus : fsShell.lsr("/springhdfs")) {
            System.out.println("> " + fileStatus.getPath());
        }
    }

    public static void main(String[] args) {
        SpringApplication.run(SpringBootHDFSApp.class, args);
    }
}
