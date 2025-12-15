package com.github.liyibo1110.hadoop.service;

import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Value;

/**
 * 通用的Service
 * @author liyibo
 * @date 2025-12-15 11:34
 */
public class BaseService {
    @Value("${hadoop.fs.defaultFS}")
    protected String hdfsUri;
    @Value("${hadoop.mapreduce.cross-platform}")
    protected String crossPlatform;
    @Value("${hadoop.mapreduce.framework.name}")
    protected String mapReduceFrameworkName;
    @Value("${hadoop.yarn.resourcemanager.address}")
    protected String resourceManagerAddress;
    @Value("${hadoop.yarn.resourcemanager.scheduler.address}")
    protected String resourceManagerSchedulerAddress;

    /**
     * 创建并初始化JobConfig实例
     * @return Job对应的Config实例
     */
    protected Configuration createAndInitJobConfig() {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", this.hdfsUri);
        conf.set("mapreduce.app-submission.cross-platform", this.crossPlatform);
        conf.set("mapreduce.framework.name", this.mapReduceFrameworkName);
        conf.set("yarn.resourcemanager.address", this.resourceManagerAddress);
        conf.set("yarn.resourcemanager.scheduler.address", this.resourceManagerSchedulerAddress);
        // 上面是必须要配置的，否则不能正常运行任务，以下的不用配置
        // conf.set("hadoop.job.user", "hadoop");
        // conf.set("mapreduce.jobtracker.address", "master:9001");
        // conf.set("yarn.resourcemanager.hostname", "master");
        // conf.set("yarn.resourcemanager.resource-tracker.address", "master:8031");
        // conf.set("yarn.resourcemanager.admin.address", "master:8033");
        // conf.set("mapreduce.jobhistory.address", "master:10020");
        // conf.set("yarn.nodemanager.aux-services", "mapreduce_shuffle");
        return conf;
    }
}
