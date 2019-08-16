package spark.spring;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.io.IOException;

/**
 * @ClassName SpringHadoopHDFSApp
 * @Description TODO
 * @Author Spark
 * @Date 8/16/2019 3:49 PM
 * @Version 1.0
 **/
public class SpringHadoopHDFSApp {

    private ApplicationContext ctx;
    private FileSystem fileSystem;

    @Test
    @Ignore
    public void testMkdirs() throws Exception{
        fileSystem.mkdirs(new Path("/springhdfs"));
    }

    @Test
    public void testText() throws IOException {
        FSDataInputStream in = null;
        try {
            in = fileSystem.open(new Path("/hdfsapi/test/a.txt"));
            IOUtils.copyBytes(in, System.out, 1024);
        }finally {
            in.close();
        }
    }

    @Before
    public void setUp(){
        ctx = new ClassPathXmlApplicationContext("beans.xml");
        fileSystem = (FileSystem) ctx.getBean("fileSystem");
    }

    @After
    public void tearDown() throws IOException {
        ctx = null;
        fileSystem.close();
    }
}
