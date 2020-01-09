package spark.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @ClassName PartitionerCountApp
 * @Description
 * http://192.168.4.128:50070   ---    Namenode
 * http://192.168.4.128:8088    ---    Application
 * http://192.168.4.128:19888   ---    JobHistory
 *
 * @Author Spark
 * @Date 8/15/2019 10:50 AM
 * @Version 1.0
 **/
public class PartitionerApp {

    /**
     * 读取输入文件
     */
    public static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable>{

        LongWritable one = new LongWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] words = line.split(" ");
            context.write(new Text(words[0]), new LongWritable(Long.parseLong(words[1])));

        }
    }

    /**
     * 归并操作
     */
    public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable>{
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            Long sum = 0L;
            for(LongWritable value : values){
                sum += value.get();
            }
            context.write(key, new LongWritable(sum));
        }
    }

    public static class MyPartitioner extends Partitioner<Text, LongWritable>{

        @Override
        public int getPartition(Text key, LongWritable value, int i) {
            if (key.toString().equals("xiaomi")) {
                return 0;
            }
            if (key.toString().equals("huawei")) {
                return 1;
            }
            if (key.toString().equals("iphone7")) {
                return 2;
            }
            return 3;
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        //清除输出文件
        Path path = new Path(args[1]);
        FileSystem fileSystem = FileSystem.get(configuration);
        if(fileSystem.exists(path)) {
            fileSystem.delete(path, true);
            System.out.println("Output file exists,but it has been deleted");
        }
        Job job = Job.getInstance(configuration, "wordcount");
        //设置job处理类
        job.setJarByClass(PartitionerApp.class);
        //设置作业处理输入路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        //设置map相关参数
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        //设置reduce相关参数
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        //设置job的partitioner
        job.setPartitionerClass(MyPartitioner.class);
        job.setNumReduceTasks(4);
        //设置作业处理的输出路径
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
