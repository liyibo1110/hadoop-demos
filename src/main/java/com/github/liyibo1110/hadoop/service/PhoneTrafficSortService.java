package com.github.liyibo1110.hadoop.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 文本文件中特定手机号的上下行流量排序
 * @author liyibo
 * @date 2025-12-12 18:11
 */
@Service
public class PhoneTrafficSortService extends BaseService {
    private static final Logger logger = LoggerFactory.getLogger(PhoneTrafficSortService.class);

    public void runJob(String inputPath, String outputPath) throws Exception {
        Configuration conf = this.createAndInitJobConfig();
        Job job = Job.getInstance(conf, "Phone Traffic Sort");
        job.setJar("D:\\ideaSource\\hadoop-demos\\target\\hadoop-demos-1.0.0-mr.jar");
        // mapper
        job.setMapperClass(PhoneTrafficSortService.SortMapper.class);
        job.setMapOutputKeyClass(PhoneTrafficSortKey.class);
        job.setMapOutputValueClass(Text.class);
        // reducer
        job.setReducerClass(PhoneTrafficSortService.SortReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PhoneTrafficSortKey.class);

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
            throw new IOException("Phone Traffic Sort job failed.");
    }

    public static class SortMapper extends Mapper<Object, Text, PhoneTrafficSortKey, Text> {
        Text phone = new Text();
        PhoneTrafficSortKey sortKey = new PhoneTrafficSortKey();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(" ");
            phone.set(tokens[0]);
            sortKey.setUp(Integer.parseInt(tokens[1]));
            sortKey.setDown(Integer.parseInt(tokens[2]));
            logger.info("{} : {}, up: {} == {}", phone, sortKey, tokens[1], sortKey.getUp());
            context.write(sortKey, phone);
        }
    }

    public static class SortReducer extends Reducer<PhoneTrafficSortKey, Text, Text, PhoneTrafficSortKey> {
        @Override
        public void reduce(PhoneTrafficSortKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for(Text value : values) {
                logger.info("{}, {}", value, key);
                context.write(value, key);
            }
        }
    }



    public static class PhoneTrafficSortKey implements WritableComparable<PhoneTrafficSortKey> {
        private int up;
        private int down;
        private int sum;

        public PhoneTrafficSortKey() {

        }

        public PhoneTrafficSortKey(int up, int down) {
            this.up = up;
            this.down = down;
            this.sum = up + down;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(this.up);
            out.writeInt(this.down);
            out.writeInt(this.sum);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            this.up = in.readInt();
            this.down = in.readInt();
            this.sum = in.readInt();
        }

        @Override
        public int compareTo(PhoneTrafficSortKey o) {
            if((this.up - o.up) == 0) // 上行流量相等，则比较下行流量
                return o.down - this.down;  // 下行按降序排
            else
                return this.up - o.up;   // 上行按升序排
        }

        @Override
        public String toString() {
            return this.up + "\t" + this.down + "\t" + this.sum;
        }

        public int getUp() {
            return up;
        }

        public void setUp(int up) {
            this.up = up;
        }

        public int getDown() {
            return down;
        }

        public void setDown(int down) {
            this.down = down;
        }

        public int getSum() {
            return sum;
        }

        public void setSum(int sum) {
            this.sum = sum;
        }
    }
}
