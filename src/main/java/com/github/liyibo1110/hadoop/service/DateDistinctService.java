package com.github.liyibo1110.hadoop.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;

/**
 * 将文本文件中特定日期去重
 * @author liyibo
 * @date 2025-12-15 16:12
 */
@Service
public class DateDistinctService extends BaseService {
    private static final Logger logger = LoggerFactory.getLogger(DateDistinctService.class);

    public void runJob(String inputPath, String outputPath) throws Exception {
        Configuration conf = this.createAndInitJobConfig();
        Job job = Job.getInstance(conf, "Date Distinct");
        job.setJar("D:\\ideaSource\\hadoop-demos\\target\\hadoop-demos-1.0.0-mr.jar");
        job.setMapperClass(DateDistinctService.DateDistinctMapper.class);
        job.setReducerClass(DateDistinctService.DateDistinctReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

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
            throw new IOException("Date Distinct job failed.");
    }

    public static class DateDistinctMapper extends Mapper<Object, Text, Text, NullWritable> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(" ");
            Text date = new Text(tokens[0]);
            context.write(date, NullWritable.get());
        }
    }

    public static class DateDistinctReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
        @Override
        public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
        }
    }
}
