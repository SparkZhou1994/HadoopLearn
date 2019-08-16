package spark.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;

/**
 * @ClassName HDFSApp
 * @Description HDFS JAVA API
 * @Author Spark
 * @Date 8/14/2019 1:17 PM
 * @Version 1.0
 **/
public class HDFSApp {

    public static final String HDFS_PATH = "hdfs://192.168.4.128:8020";

    FileSystem fileSystem = null;
    Configuration configuration = null;

    /**
     * 创建HDFS目录
     * @throws Exception
     */
    @Test
    @Ignore
    public void mkdir() throws Exception{
        fileSystem.mkdirs(new Path("/hdfsapi/test"));
    }

    /**
     * 创建文件
     * @throws Exception
     */
    @Test
    @Ignore
    public void create() throws Exception{
        FSDataOutputStream output = null;
        try {
            output = fileSystem.create(new Path("/hdfsapi/test/a.txt"));
            output.write("hello hadoop".getBytes());
            output.flush();
        } finally {
            output.close();
        }
    }

    /**
     * 查看文件内容
     * @throws Exception
     */
    @Test
    @Ignore
    public void cat() throws Exception{
        FSDataInputStream in = null;
        try {
            in = fileSystem.open(new Path("/hdfsapi/test/a.txt"));
            IOUtils.copyBytes(in, System.out, 1024);
        }finally {
            in.close();
        }
    }

    /**
     * 文件重命名
     * @throws Exception
     */
    @Test
    @Ignore
    public void rename() throws Exception{
        Path oldPath = new Path("/hdfsapi/test/a.txt");
        Path newPath = new Path("/hdfsapi/test/b.txt");
        fileSystem.rename(oldPath, newPath);
    }

    /**
     * 上传文件至HDFS
     * @throws Exception
     */
    @Test
    @Ignore
    public void copyFromLocalFile() throws Exception{
        Path localPath = new Path("e://Hadoop Operation Document.txt");
        Path hdfsPath = new Path("/hdfsapi/test");
        fileSystem.copyFromLocalFile(localPath, hdfsPath);
    }

    /**
     * 带.进度条的上传
     * @throws Exception
     */
    @Test
    @Ignore
    public void copyFromLocalFileWithProgress() throws Exception{
        InputStream in = null;
        FSDataOutputStream out = null;
        try {
            in = new BufferedInputStream(new FileInputStream(
                    new File("e://Hadoop Operation Document.txt")));
            out = fileSystem.create(new Path("/hdfsapi/test"),
                    new Progressable() {
                        @Override
                        public void progress() {
                            System.out.println("."); //设置进度条
                        }
                    });
            IOUtils.copyBytes(in, out, 4096);
        }finally {
            in.close();
        }
    }

    /**
     * 下载HDFS文件
     * @throws Exception
     */
    @Test
    @Ignore
    public void copyToLocalFile() throws Exception{
        Path localPath = new Path("e://");
        Path hdfsPath = new Path("/hdfsapi/test/b.txt");
        fileSystem.copyToLocalFile(false,hdfsPath, localPath,true);
    }

    /**
     * 查看某个目录下的所有文件
     * @throws Exception
     */
    @Test
    @Ignore
    public void listFiles() throws Exception{
        FileStatus[] statuses = fileSystem.listStatus(new Path("/hdfsapi/test"));
        for(FileStatus fileStatus : statuses) {
            String isDir = fileStatus.isDirectory()?"文件夹":"文件";
            Short replication = fileStatus.getReplication();
            Long len = fileStatus.getLen();
            String path = fileStatus.getPath().toString();
            System.out.println(isDir + "\t" + replication + "\t" + len + "\t" + path);
        }
    }

    @Test
    public void delete() throws Exception{
        fileSystem.delete(new Path("/hdfsapi/test"), true);
    }

    @Before
    public void setUp() throws Exception{
        System.out.println("HDFSApp.setUp");
        configuration = new Configuration();
        fileSystem = FileSystem.get(new URI(HDFS_PATH), configuration,"hadoop0");
    }

    @After
    public void tearDown() throws Exception{
        configuration = null;
        fileSystem = null;
        System.out.println("HDFSApp.tearDown");
    }
}
