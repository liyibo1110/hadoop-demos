package com.github.liyibo1110.hadoop.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Arrays;

/**
 * 原始数据为friend_relationships.txt，计算哪些人两两之间的共同好友
 * 特点是需要2次MapReduce，涉及到了不同Job的串行，后面的Job依赖前面的Job的计算结果
 * @author liyibo
 * @date 2025-12-16 15:34
 */
@Service
public class MutualFriendService extends BaseService {
    private static final Logger logger = LoggerFactory.getLogger(OrderProductPriceMaxService.class);

    public void runJob(String inputPath, String outputPath, String mergedPath) throws Exception {
        Configuration conf = this.createAndInitJobConfig();
        Job job1 = Job.getInstance(conf, "Decompose");
        job1.setJar("D:\\ideaSource\\hadoop-demos\\target\\hadoop-demos-1.0.0-mr.jar");
        // mapper
        job1.setMapperClass(DecomposeMapper.class);
        // reducer
        job1.setReducerClass(DecomposeReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job1, new Path(inputPath));
        FileOutputFormat.setOutputPath(job1, new Path(outputPath));

        // 每次要删除之前生成的输出
        FileSystem fs = FileSystem.get(conf);
        Path outputDir = new Path(outputPath);
        if(fs.exists(outputDir))
            fs.delete(outputDir, true);

        // 构建Job控制容器
        ControlledJob cj1 = new ControlledJob(conf);
        cj1.setJob(job1); // 加入job1

        Job job2 = Job.getInstance(conf, "Merge");
        job2.setJar("D:\\ideaSource\\hadoop-demos\\target\\hadoop-demos-1.0.0-mr.jar");
        // mapper
        job2.setMapperClass(MergeMapper.class);
        // reducer
        job2.setReducerClass(MergeReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job2, new Path(outputPath));
        FileOutputFormat.setOutputPath(job2, new Path(mergedPath));

        // 每次要删除之前生成的输出
        Path mergedDir = new Path(mergedPath);
        if(fs.exists(mergedDir))
            fs.delete(mergedDir, true);

        ControlledJob cj2 = new ControlledJob(conf);
        cj2.setJob(job2);

        // job2在job1之后执行
        cj2.addDependingJob(cj1);

        // 构建作业主控制器
        JobControl jc = new JobControl("JobController");
        jc.addJob(cj1);
        jc.addJob(cj2);
        // 执行任务
        new Thread(jc).start();
        while(true) {
            if(jc.allFinished()) {
                logger.info(jc.getSuccessfulJobList().toString());
                jc.stop();
                break;
            }
        }
    }

    /**
     * 将A:B,C,D,F,E,O，拆分两两一对，即B -> A, C -> A，代表相互是好友关系
     */
    public static class DecomposeMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String strs = value.toString();
            Text person = new Text(strs.substring(0, 1));
            String[] friends = strs.substring(2).split(",");
            for(int i = 0; i < friends.length; i++)
                context.write(new Text(friends[i]), person);
        }
    }

    public static class DecomposeReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();
            for(Text value : values)
                sb.append(value.toString()).append(",");
            sb.deleteCharAt(sb.length() - 1);
            // 输出1个人的所有好友
            context.write(key, new Text(sb.toString()));
        }
    }

    /**
     * 接上一个Reducer的输出，将A B,C,E遍历输出为B-C -> A，即B和C的共同好友有A
     */
    public static class MergeMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Text person = new Text(value.toString().substring(0, 1));
            String[] friends = value.toString().substring(2).split(",");
            Arrays.sort(friends);   // 要排序，不然A-B,B-A不能归并到一起
            // 对A B,C,E遍历输出如B-C -> A
            for(int i = 0; i < friends.length; i++) {
                for(int j = 0; j < friends.length; j++) {
                    String twoFriends = friends[i] + "-" + friends[j];
                    context.write(new Text(twoFriends), person);
                }
            }
        }
    }

    public static class MergeReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();
            for(Text value : values)
                sb.append(value.toString()).append(",");
            sb.deleteCharAt(sb.length() - 1);
            context.write(key, new Text(sb.toString()));
        }
    }
}
