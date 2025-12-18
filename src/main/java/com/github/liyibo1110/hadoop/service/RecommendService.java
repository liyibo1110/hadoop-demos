package com.github.liyibo1110.hadoop.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * 原始数据为ali_t.csv，计算并推荐商品
 * 需要多次MapReduce串行
 * @author liyibo
 * @date 2025-12-17 12:09
 */
@Service
public class RecommendService extends BaseService {
    private static final Logger logger = LoggerFactory.getLogger(RecommendService.class);

    public void runJob() throws Exception {
        Configuration conf = this.createAndInitJobConfig();
        FileSystem fs = FileSystem.get(conf);

        Job job1 = Job.getInstance(conf, "Step 1");
        job1.setJar("D:\\ideaSource\\hadoop-demos\\target\\hadoop-demos-1.0.0-mr.jar");
        // mapper
        job1.setMapperClass(Step1Mapper.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(NullWritable.class);
        // reducer
        job1.setReducerClass(Step1Reducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job1, new Path("/recommend/input/ali_t.csv"));
        Path outputDir1 = new Path("/recommend/output/step1");
        if(fs.exists(outputDir1))
            fs.delete(outputDir1, true);
        FileOutputFormat.setOutputPath(job1, outputDir1);

        // 构建Job控制容器
        ControlledJob cj1 = new ControlledJob(conf);
        cj1.setJob(job1); // 加入job1


        Job job2 = Job.getInstance(conf, "Step 2");
        job2.setJar("D:\\ideaSource\\hadoop-demos\\target\\hadoop-demos-1.0.0-mr.jar");
        // mapper
        job2.setMapperClass(Step2Mapper.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        // reducer
        job2.setReducerClass(Step2Reducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job2, new Path("/recommend/output/step1"));
        Path outputDir2 = new Path("/recommend/output/step2");
        if(fs.exists(outputDir2))
            fs.delete(outputDir2, true);
        FileOutputFormat.setOutputPath(job2, outputDir2);

        ControlledJob cj2 = new ControlledJob(conf);
        cj2.setJob(job2);

        // job2在job1之后执行
        cj2.addDependingJob(cj1);


        Job job3 = Job.getInstance(conf, "Step 3");
        job3.setJar("D:\\ideaSource\\hadoop-demos\\target\\hadoop-demos-1.0.0-mr.jar");
        // mapper
        job3.setMapperClass(Step3Mapper.class);
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(IntWritable.class);
        // reducer
        job3.setReducerClass(Step3Reducer.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(IntWritable.class);
        // combiner
        job3.setCombinerClass(Step3Reducer.class);

        FileInputFormat.addInputPath(job3, new Path("/recommend/output/step2"));
        Path outputDir3 = new Path("/recommend/output/step3");
        if(fs.exists(outputDir3))
            fs.delete(outputDir3, true);
        FileOutputFormat.setOutputPath(job3, outputDir3);

        ControlledJob cj3 = new ControlledJob(conf);
        cj3.setJob(job3);

        // job3在job2之后执行
        cj3.addDependingJob(cj2);


        Job job4 = Job.getInstance(conf, "Step 4");
        job4.setJar("D:\\ideaSource\\hadoop-demos\\target\\hadoop-demos-1.0.0-mr.jar");
        // mapper
        job4.setMapperClass(Step4Mapper.class);
        job4.setMapOutputKeyClass(Text.class);
        job4.setMapOutputValueClass(Text.class);
        // reducer
        job4.setReducerClass(Step4Reducer.class);
        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(Text.class);

        /** 注意step4是要有2个输入，分别要读取step2和step3的输出 */
        FileInputFormat.addInputPath(job4, new Path("/recommend/output/step2"));
        FileInputFormat.addInputPath(job4, new Path("/recommend/output/step3"));
        Path outputDir4 = new Path("/recommend/output/step4");
        if(fs.exists(outputDir4))
            fs.delete(outputDir4, true);
        FileOutputFormat.setOutputPath(job4, outputDir4);

        ControlledJob cj4 = new ControlledJob(conf);
        cj4.setJob(job4);

        // job4在job3之后执行
        cj4.addDependingJob(cj3);


        Job job5 = Job.getInstance(conf, "Step 5");
        job5.setJar("D:\\ideaSource\\hadoop-demos\\target\\hadoop-demos-1.0.0-mr.jar");
        // mapper
        job5.setMapperClass(Step5Mapper.class);
        job5.setMapOutputKeyClass(Text.class);
        job5.setMapOutputValueClass(Text.class);
        // reducer
        job5.setReducerClass(Step5Reducer.class);
        job5.setOutputKeyClass(Text.class);
        job5.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job5, new Path("/recommend/output/step4"));
        Path outputDir5 = new Path("/recommend/output/step5");
        if(fs.exists(outputDir5))
            fs.delete(outputDir5, true);
        FileOutputFormat.setOutputPath(job5, outputDir5);

        ControlledJob cj5 = new ControlledJob(conf);
        cj5.setJob(job5);

        // job5在job4之后执行
        cj5.addDependingJob(cj4);


        Job job6 = Job.getInstance(conf, "Step 6");
        job6.setJar("D:\\ideaSource\\hadoop-demos\\target\\hadoop-demos-1.0.0-mr.jar");
        // mapper
        job6.setMapperClass(Step6Mapper.class);
        job6.setMapOutputKeyClass(PairWritable.class);
        job6.setMapOutputValueClass(Text.class);
        // reducer
        job6.setReducerClass(Step6Reducer.class);
        job6.setOutputKeyClass(Text.class);
        job6.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job6, new Path("/recommend/output/step5"));
        Path outputDir6 = new Path("/recommend/output/step6");
        if(fs.exists(outputDir6))
            fs.delete(outputDir6, true);
        FileOutputFormat.setOutputPath(job6, outputDir6);

        ControlledJob cj6 = new ControlledJob(conf);
        cj6.setJob(job6);

        // job6在job5之后执行
        cj6.addDependingJob(cj5);


        // 构建作业主控制器
        JobControl jc = new JobControl("JobController");
        jc.addJob(cj1);
        jc.addJob(cj2);
        jc.addJob(cj3);
        jc.addJob(cj4);
        jc.addJob(cj5);
        jc.addJob(cj6);

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
     * 去重每行相同的数据
     */
    public static class Step1Mapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if(key.get() != 0)
                context.write(value, NullWritable.get());
        }
    }

    /**
     * 相当于什么也没干
     */
    public static class Step1Reducer extends Reducer<Text, NullWritable, Text, NullWritable> {
        public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
        }
    }

    /**
     * Step2最终是按用户分组，计算所有产品出现的组合列表，得到用户对物品的喜爱度的得分矩阵
     * u13 i160:1,
     * u14 i25:1,i223:1,
     * u16 i252:1,
     * u21 i266:1,
     * u24 i64:1,i218:1,i185:1,
     * u26 i276:1,i201:1,i348:1,i321:1,i136:1,
     */

    /**
     * u14 i25:1
     * u14 i223:1
     */
    public static class Step2Mapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(",");
            String user = tokens[0];
            String item = tokens[1];
            String action = tokens[2];
            Text k = new Text(user);
            Integer rv = Integer.parseInt(action) + 1;
            Text v = new Text(item + ":" + rv.intValue());
            context.write(k, v);
        }
    }

    public static class Step2Reducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<String, Integer> r = new HashMap<>();   // 就是个action的累加器
            for(Text value : values) {
                String[] vs = value.toString().split(":");
                String item = vs[0];
                Integer action = Integer.parseInt(vs[1]);
                // 累加权重
                action = ((Integer)(r.get(item) == null ? 0 : r.get(item))).intValue() + action;
                r.put(item, action);
            }
            StringBuilder sb = new StringBuilder();
            for(var entry : r.entrySet())
                sb.append(entry.getClass() + ":" + entry.getValue().intValue() + ",");
            context.write(key, new Text(sb.toString()));
        }
    }

    /**
     * Step3最终是对物品组合列表进行计数，建立产品的同现矩阵，即产品和其它产品的相关度即类似WordCount
     * i100:i100	3
     * i100:i105	1
     * i100:i106	1
     * i100:i109	1
     * i100:i114	1
     * i100:i124	1
     */

    /**
     * 输入的Step2的最终输出
     * u13 i160:1,
     * u14 i25:1,i223:1,
     * u16 i252:1,
     * u21 i266:1,
     * u24 i64:1,i218:1,i185:1,
     * u26 i276:1,i201:1,i348:1,i321:1,i136:1
     */
    public static class Step3Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private static final Text K = new Text();
        private static final IntWritable V = new IntWritable(1);

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            logger.info("step 3 mapper input value: {}", value.toString());
            String[] tokens = value.toString().split("\t");
            String[] items = tokens[1].split(",");  // i160:1
            for(int i = 0; i < items.length; i++) {
                String itemA = items[i].split(":")[0];
                for(int j = 0; j < items.length; j++) {
                    String itemB = items[j].split(":")[0];
                    String k = itemA + ":" + itemB;
                    // logger.info("step 3 mapper key: {}", k);
                    K.set(k);
                    context.write(K, V);    // 相当于类似WordCount计算过程
                }
            }
        }
    }

    /**
     * 累加sum
     */
    public static class Step3Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private static final IntWritable V = new IntWritable(1);
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for(IntWritable value : values)
                sum += value.get();
            V.set(sum);
            // logger.info("step 3 reducer key: {}", key);
            // logger.info("step 3 reducer value: {}", sum);
            context.write(key, V);
        }
    }

    /**
     * Step4是要把同现矩阵（Step3的输出）和得分矩阵（Step2的输出）相乘
     */


    public static class Step4Mapper extends Mapper<LongWritable, Text, Text, Text> {
        /** 同现矩阵 or 得分矩阵 */
        private String flag;

        /**
         * 初始化调用
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            FileSplit split = (FileSplit)context.getInputSplit();
            this.flag = split.getPath().getParent().getName();
            logger.info("flag: {}", this.flag);
        }

        /**
         * 输出格式为：
         * key：产品ID
         * value：A:同现矩阵 B:得分矩阵
         */
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = Pattern.compile("[\t,]").split(value.toString());
            if(this.flag.equals("output3")) { // 同现矩阵
                /**
                 * i100:i181 1
                 * i100:i184 2
                 */
                String[] v1 = tokens[0].split(":");
                String itemId1 = v1[0];
                String itemId2 = v1[1];
                String num = tokens[1];
                Text k = new Text(itemId1);
                Text v = new Text("A:" + itemId2 + "," + num);
                context.write(k, v);
            }else if(this.flag.equals("output2")) { // 得分矩阵
                /**
                 * u24 i64:1,i218:1,i185:1,
                 */
                String userId = tokens[0];
                for(int i = 0; i < tokens.length; i++) {
                    String[] vector = tokens[i].split(":");
                    String itemId = vector[0];  // 产品id
                    String pref = vector[1]; // 喜爱分数
                    Text k = new Text(itemId);
                    Text v = new Text("B:" + userId + "," + pref);
                    context.write(k, v);
                }
            }
        }
    }

    /**
     * 结果为：u2723 i9,8.0
     * 即每个用户对每个产品的喜爱分数
     */
    public static class Step4Reducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<String, Integer> mapA = new HashMap<>();
            Map<String, Integer> mapB = new HashMap<>();

            for(Text line : values) {
                String val = line.toString();
                if(val.startsWith("A:")) {  // 同现矩阵
                    String[] kv = Pattern.compile("[\t,]").split(val.substring(2));
                    try {
                        mapA.put(kv[0], Integer.parseInt(kv[1]));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }else if(val.startsWith("B:")) {    // 得分矩阵
                    String[] kv = Pattern.compile("[\t,]").split(val.substring(2));
                    try {
                        mapB.put(kv[0], Integer.parseInt(kv[1]));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }

            double result = 0D;
            Iterator<String> iterA = mapA.keySet().iterator();
            while(iterA.hasNext()) {
                String mapK = iterA.next();  // 产品id
                int num = mapA.get(mapK).intValue(); // 同时出现的次数
                Iterator<String> iterB = mapB.keySet().iterator();
                while(iterB.hasNext()) {
                    String mapKB = iterB.next();    // userId
                    int pref = mapB.get(mapKB).intValue();  // 喜爱分数
                    result = num * pref; // 矩阵乘法计算

                    Text k = new Text(mapKB);
                    Text v = new Text(mapK + "," + result);
                    context.write(k, v);
                }
            }
        }
    }

    /**
     * Step5是要把Step4相乘之后的矩阵，再相加获得结果矩阵
     */

    public static class Step5Mapper extends Mapper<LongWritable, Text, Text, Text> {

        /**
         * u2723 i9,8.0
         * 其实并没有作任何变化，原样输出
         */
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = Pattern.compile("[\t,]").split(value.toString());
            Text k = new Text(tokens[0]);    // userId
            Text v = new Text(tokens[1] + "," + tokens[2]);
            context.write(k, v);
        }
    }

    /**
     * 矩阵累加
     */
    public static class Step5Reducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<String, Double> map = new HashMap<>();
            for(Text line : values) {   // i9,8.0
                String[] tokens = line.toString().split(",");
                String itemId = tokens[0];
                Double score = Double.parseDouble(tokens[1]);
                if(map.containsKey(itemId))
                    map.put(itemId, map.get(itemId) + score);
                else
                    map.put(itemId, score);
            }
            for(String itemId : map.keySet()) {
                double score = map.get(itemId);
                Text v = new Text(itemId + "," + score);
                context.write(key, v);
            }
        }
    }

    /**
     * Step6是负责排序，按照推荐得分降序排序，每个用户最终列出10个推荐物品
     */

    public static class Step6Mapper extends Mapper<LongWritable, Text, PairWritable, Text> {
        private static final Text V = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = Pattern.compile("[\t,]]").split(value.toString());
            PairWritable k = new PairWritable();
            k.setUserId(tokens[0]);
            k.setNum(Double.parseDouble(tokens[2]));
            V.set(tokens[1] + ":" + tokens[2]);
            context.write(k, V);
        }
    }

    public static class Step6Reducer extends Reducer<PairWritable, Text, Text, Text> {
        private static final Text K = new Text();
        private static final Text V = new Text();
        public void reduce(PairWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int i = 0;
            StringBuilder sb = new StringBuilder();
            for(Text value : values) {
                if(i == 10) // 只取前10条
                    break;
                sb.append(values.toString() + ",");
                i++;
            }
            K.set(key.getUserId());
            V.set(sb.toString());
            context.write(K, V);
        }
    }

    public static class PairWritable implements WritableComparable<PairWritable> {
        private String userId;
        private double num;

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeUTF(this.userId);
            out.writeDouble(this.num);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            this.userId = in.readUTF();
            this.num = in.readDouble();
        }

        @Override
        public int compareTo(PairWritable o) {
            int r = this.userId.compareTo(o.getUserId());
            if(r == 0)
                return Double.compare(this.num, o.getNum());
            return r;
        }

        public String getUserId() {
            return userId;
        }

        public void setUserId(String userId) {
            this.userId = userId;
        }

        public double getNum() {
            return num;
        }

        public void setNum(double num) {
            this.num = num;
        }
    }
}
