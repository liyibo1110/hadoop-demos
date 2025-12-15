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
import org.springframework.stereotype.Service;

import java.io.IOException;

/**
 * 统计文本文件中ip各自出现的次数（和WordCount基本差不多）
 * 没有用到什么特别的额外特性
 * @author liyibo
 * @date 2025-12-12 10:46
 */
@Service
public class LogAnalysisService extends BaseService {
    public void runJob(String inputPath, String outputPath) throws Exception {
        Configuration conf = this.createAndInitJobConfig();
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
