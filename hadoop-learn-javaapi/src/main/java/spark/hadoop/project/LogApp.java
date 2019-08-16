package spark.hadoop.project;

import com.kumkee.userAgent.UserAgent;
import com.kumkee.userAgent.UserAgentParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @ClassName LogApp
 * @Description TODO
 * @Author Spark
 * @Date 8/16/2019 1:47 PM
 * @Version 1.0
 **/
public class LogApp {

    /**
     * 读取输入文件
     */
    public static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

        LongWritable one = new LongWritable(1);

        private UserAgentParser userAgentParser = new UserAgentParser();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            userAgentParser = new UserAgentParser();
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            userAgentParser = null;
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //日志信息
            String line = value.toString();
            String source = line.substring(getCharacterPosition(line, "\"", 7)) + 1;
            UserAgent agent = userAgentParser.parse(source);
            String browser = agent.getBrowser();
            context.write(new Text(browser), one);
        }
    }

    /**
     * 归并操作
     */
    public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            Long sum = 0L;
            for(LongWritable value : values){
                sum += value.get();
            }
            context.write(key, new LongWritable(sum));
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
        job.setJarByClass(LogApp.class);
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
        //通过job设置combiner处理类,其实逻辑上和我们的reducer是一模一样的
        //combiner 只适用于求和
        job.setCombinerClass(MyReducer.class);
        //设置作业处理的输出路径
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    private static Integer getCharacterPosition(String value, String operator, Integer index) {
        Matcher slashMatcher = Pattern.compile(operator).matcher(value);
        Integer mIdx = 0;
        while(slashMatcher.find()) {
            mIdx ++;
            if (mIdx == index) {
                break;
            }
        }
        return slashMatcher.start();
    }
}
