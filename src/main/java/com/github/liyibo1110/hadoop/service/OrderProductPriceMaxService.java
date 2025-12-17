package com.github.liyibo1110.hadoop.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
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
 * 统计每个订单的最大金额
 * 分组 + 取极值
 * @author liyibo
 * @date 2025-12-16 14:31
 */
@Service
public class OrderProductPriceMaxService extends BaseService {
    private static final Logger logger = LoggerFactory.getLogger(OrderProductPriceMaxService.class);

    public void runJob(String inputPath, String outputPath) throws Exception {
        Configuration conf = this.createAndInitJobConfig();
        Job job = Job.getInstance(conf, "Order Product Price Max");
        job.setJar("D:\\ideaSource\\hadoop-demos\\target\\hadoop-demos-1.0.0-mr.jar");
        // mapper
        job.setMapperClass(GroupMapper.class);
        job.setMapOutputKeyClass(Pair.class);
        job.setMapOutputValueClass(NullWritable.class);
        // reducer
        job.setReducerClass(GroupReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        // 自定义分组类
        job.setGroupingComparatorClass(GroupComparator.class);

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

    public static class GroupMapper extends Mapper<Object, Text, Pair, NullWritable> {
        Pair pair = new Pair();
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(" ");
            this.pair.setOrderId(tokens[0]);
            this.pair.setPrice(new DoubleWritable(Double.parseDouble(tokens[2])));
            logger.info("{}: {}", this.pair.getOrderId(), this.pair.getPrice());
            context.write(this.pair, NullWritable.get());
        }
    }

    public static class GroupReducer extends Reducer<Pair, NullWritable, Text, DoubleWritable> {
        @Override
        public void reduce(Pair key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            logger.info("{} : {}", key.getOrderId(), key.getPrice());
            context.write(new Text(key.getOrderId()), key.getPrice());
        }
    }

    public static class Pair implements WritableComparable<Pair> {
        private String orderId;
        private DoubleWritable price;

        public Pair() {

        }

        public Pair(String orderId, DoubleWritable price) {
            this.orderId = orderId;
            this.price = price;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeUTF(this.orderId);
            out.writeDouble(this.price.get());
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            this.orderId = in.readUTF();
            this.price = new DoubleWritable(in.readDouble());
        }

        @Override
        public int compareTo(Pair o) {
            if(this.orderId.equals(o.orderId))  // orderId相同，按照price降序，所以最大值会在前面
                return o.price.compareTo(this.price);
            else
                return this.orderId.compareTo(o.orderId);
        }

        public String getOrderId() {
            return orderId;
        }

        public void setOrderId(String orderId) {
            this.orderId = orderId;
        }

        public DoubleWritable getPrice() {
            return price;
        }

        public void setPrice(DoubleWritable price) {
            this.price = price;
        }
    }

    public static class GroupComparator extends WritableComparator {
        public GroupComparator() {
            super(Pair.class, true);
        }

        /** 按照orderId进行分组，即不同分组才可能对应不同的reducer */
        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            Pair pa = (Pair)a;
            Pair pb = (Pair)b;
            return pa.getOrderId().compareTo(pb.getOrderId());
        }
    }
}
