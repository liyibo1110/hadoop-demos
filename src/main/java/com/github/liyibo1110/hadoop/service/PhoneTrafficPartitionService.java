package com.github.liyibo1110.hadoop.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * 文本文件中特定手机号的上下行流量分区，即特定前缀的手机号，归到特定的文件中
 * @author liyibo
 * @date 2025-12-12 18:11
 */
@Service
public class PhoneTrafficPartitionService extends BaseService {
    private static final Logger logger = LoggerFactory.getLogger(PhoneTrafficPartitionService.class);

    public void runJob(String inputPath, String outputPath) throws Exception {
        Configuration conf = this.createAndInitJobConfig();
        Job job = Job.getInstance(conf, "Phone Traffic Partition");
        job.setJar("D:\\ideaSource\\hadoop-demos\\target\\hadoop-demos-1.0.0-mr.jar");
        // mapper
        job.setMapperClass(PhoneTrafficMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PhoneTrafficWritable.class);
        // reducer
        job.setReducerClass(PhoneTrafficReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PhoneTrafficWritable.class);
        // partition
        job.setPartitionerClass(PhoneTrafficPartitioner.class);
        job.setNumReduceTasks(5);

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
            throw new IOException("job failed.");
    }

    public static class PhoneTrafficMapper extends Mapper<Object, Text, Text, PhoneTrafficWritable> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(" ");
            Text phone = new Text(tokens[0]);
            PhoneTrafficWritable data = new PhoneTrafficWritable(Integer.parseInt(tokens[1]), Integer.parseInt(tokens[2]));
            logger.info("phone traffic data: {}", data);
            context.write(phone, data);
        }
    }

    public static class PhoneTrafficReducer extends Reducer<Text, PhoneTrafficWritable, Text, PhoneTrafficWritable> {
        @Override
        public void reduce(Text key, Iterable<PhoneTrafficWritable> values, Context context) throws IOException, InterruptedException {
            int up = 0;
            int down = 0;
            for(PhoneTrafficWritable data : values) {
                up += data.getUp();
                down += data.getDown();
            }
            logger.info("{} : {}, {}", key, up, down);
            context.write(key, new PhoneTrafficWritable(up, down));
        }
    }

    /**
     * 自定义的数据Writable实现
     */
    public static class PhoneTrafficWritable implements Writable {
        /* 特定手机号的上行流量 */
        private int up;
        /* 特定手机号的下行流量 */
        private int down;
        /* 特定手机号的总流量 */
        private int sum;

        public PhoneTrafficWritable() {

        }

        public PhoneTrafficWritable(int up, int down) {
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

    public static class PhoneTrafficPartitioner extends Partitioner<Text, PhoneTrafficWritable> {
        private static Map<String, Integer> numberDict = new HashMap<>();
        static {
            numberDict.put("133", 0);
            numberDict.put("135", 1);
            numberDict.put("137", 2);
            numberDict.put("138", 3);
        }
        @Override
        public int getPartition(Text key, PhoneTrafficWritable value, int numPartitions) {
            String prefix = key.toString().substring(0, 3);
            return numberDict.getOrDefault(prefix, 4);
        }
    }
}
