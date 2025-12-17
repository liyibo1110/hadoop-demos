package com.github.liyibo1110.hadoop.web;

import com.github.liyibo1110.hadoop.service.DateDistinctService;
import com.github.liyibo1110.hadoop.service.LogAnalysisService;
import com.github.liyibo1110.hadoop.service.MutualFriendService;
import com.github.liyibo1110.hadoop.service.OrderProductPriceMaxService;
import com.github.liyibo1110.hadoop.service.PhoneTrafficPartitionService;
import com.github.liyibo1110.hadoop.service.PhoneTrafficService;
import com.github.liyibo1110.hadoop.service.PhoneTrafficSortService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author liyibo
 * @date 2025-12-12 10:35
 */
@RestController
public class Controller {
    @Autowired
    private LogAnalysisService logAnalysisService;
    @Autowired
    private PhoneTrafficService phoneTrafficService;
    @Autowired
    private DateDistinctService dateDistinctService;
    @Autowired
    private PhoneTrafficSortService phoneTrafficSortService;
    @Autowired
    private PhoneTrafficPartitionService phoneTrafficPartitionService;
    @Autowired
    private OrderProductPriceMaxService orderProductPriceMaxService;
    @Autowired
    private MutualFriendService mutualFriendService;
    @GetMapping("/runLogAnalysis")
    public String logAnalysis(String inputPath, String outputPath) {
        // 假定数据存放在HDFS上的特定目录里
        inputPath = "/logAnalyzer/input";
        outputPath = "/logAnalyzer/output";
        try {
            this.logAnalysisService.runJob(inputPath, outputPath);
            return "ok";
        } catch (Exception e) {
            return e.getMessage();
        }
    }

    @GetMapping("/runPhoneTraffic")
    public String phoneTraffic(String inputPath, String outputPath) {
        // 假定数据存放在HDFS上的特定目录里
        inputPath = "/phoneTraffic/input";
        outputPath = "/phoneTraffic/output";
        try {
            this.phoneTrafficService.runJob(inputPath, outputPath);
            return "ok";
        } catch (Exception e) {
            return e.getMessage();
        }
    }

    @GetMapping("/runDateDistinct")
    public String dateDistinct(String inputPath, String outputPath) {
        // 假定数据存放在HDFS上的特定目录里
        inputPath = "/dateDistinct/input";
        outputPath = "/dateDistinct/output";
        try {
            this.dateDistinctService.runJob(inputPath, outputPath);
            return "ok";
        } catch (Exception e) {
            return e.getMessage();
        }
    }

    @GetMapping("/runPhoneTrafficSort")
    public String phoneTrafficSort(String inputPath, String outputPath) {
        // 假定数据存放在HDFS上的特定目录里
        inputPath = "/phoneTrafficSort/input";
        outputPath = "/phoneTrafficSort/output";
        try {
            this.phoneTrafficSortService.runJob(inputPath, outputPath);
            return "ok";
        } catch (Exception e) {
            return e.getMessage();
        }
    }

    @GetMapping("/runPhoneTrafficPartition")
    public String phoneTrafficPartition(String inputPath, String outputPath) {
        // 假定数据存放在HDFS上的特定目录里
        inputPath = "/phoneTrafficPartition/input";
        outputPath = "/phoneTrafficPartition/output";
        try {
            this.phoneTrafficPartitionService.runJob(inputPath, outputPath);
            return "ok";
        } catch (Exception e) {
            return e.getMessage();
        }
    }

    @GetMapping("/runOrderProductPriceMax")
    public String orderProductPriceMax(String inputPath, String outputPath) {
        // 假定数据存放在HDFS上的特定目录里
        inputPath = "/orderProductPriceMax/input";
        outputPath = "/orderProductPriceMax/output";
        try {
            this.orderProductPriceMaxService.runJob(inputPath, outputPath);
            return "ok";
        } catch (Exception e) {
            return e.getMessage();
        }
    }

    @GetMapping("/runMutualFriend")
    public String mutualFriend(String inputPath, String outputPath, String mergedPath) {
        // 假定数据存放在HDFS上的特定目录里
        inputPath = "/mutualFriend/input";
        outputPath = "/mutualFriend/output";
        mergedPath = "/mutualFriend/merged";
        try {
            this.mutualFriendService.runJob(inputPath, outputPath, mergedPath);
            return "ok";
        } catch (Exception e) {
            return e.getMessage();
        }
    }
}
