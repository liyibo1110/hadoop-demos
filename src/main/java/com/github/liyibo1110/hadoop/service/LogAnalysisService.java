package com.github.liyibo1110.hadoop.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;

/**
 * @author liyibo
 * @date 2025-12-12 10:46
 */
@Service
public class LogAnalysisService {
    @Value("${hadoop.fs.defaultFS}")
    private String hdfsUri;
    @Value("${hadoop.mapreduce.cross-platform}")
    private String crossPlatform;
    @Value("${hadoop.mapreduce.framework.name}")
    private String mapReduceFrameworkName;
    @Value("${hadoop.yarn.resourcemanager.address}")
    private String resourceManagerAddress;
    @Value("${hadoop.yarn.resourcemanager.scheduler.address}")
    private String resourceManagerSchedulerAddress;

    public void runJob(String inputPath, String outputPath) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", this.hdfsUri);
        conf.set("mapreduce.app-submission.cross-platform", this.crossPlatform);
        conf.set("mapreduce.framework.name", this.mapReduceFrameworkName);
        conf.set("yarn.resourcemanager.address", this.resourceManagerAddress);
        conf.set("yarn.resourcemanager.scheduler.address", this.resourceManagerSchedulerAddress);
        // 上面是必须要配置，否则不能正常运行任务，以下的不用配置
        // conf.set("hadoop.job.user", "hadoop");
        // conf.set("mapreduce.jobtracker.address", "master:9001");
        // conf.set("yarn.resourcemanager.hostname", "master");
        // conf.set("yarn.resourcemanager.resource-tracker.address", "master:8031");
        // conf.set("yarn.resourcemanager.admin.address", "master:8033");
        // conf.set("mapreduce.jobhistory.address", "master:10020");
        // conf.set("yarn.nodemanager.aux-services", "mapreduce_shuffle");

        Job job = Job.getInstance(conf, "Log Analyzer");
        // job.setJarByClass(LogAnalysisService.class);
        job.setJar("D:\\ideaSource\\hadoop-demos\\target\\hadoop-demos-1.0.0-mr.jar");
        job.setMapperClass(LogAnalyzerMapper.class);
        job.setReducerClass(LogAnalyzerReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        // 每次要删除之前生成的输出
        FileSystem fs = FileSystem.get(conf);
        Path outputDir = new Path(outputPath);
        if(fs.exists(outputDir))
            fs.delete(outputDir, true);

        // 执行任务
        boolean success = job.waitForCompletion(true);
        if(!success)
            throw new IOException("Log Analysis job failed.");
    }

    public static class LogAnalyzerMapper extends Mapper<Object, Text, Text, IntWritable> {
        private static final IntWritable one = new IntWritable(1);
        private Text ipAddress = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // value就是一行log信息，以空格来分段
            String[] tokens = value.toString().split(" ");
            if(tokens.length > 0) {
                this.ipAddress.set(tokens[0]);   // 提取ip地址（假设第1组就是ip）
                context.write(this.ipAddress, this.one);
            }
        }
    }

    public static class LogAnalyzerReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            // 只是把收到的统计值累加到一起
            int sum = 0;
            for(IntWritable value : values)
                sum += value.get();
            result.set(sum);
            context.write(key, result);
        }
    }
}
