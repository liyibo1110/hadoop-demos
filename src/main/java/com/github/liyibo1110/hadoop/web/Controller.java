package com.github.liyibo1110.hadoop.web;

import com.github.liyibo1110.hadoop.service.DateDistinctService;
import com.github.liyibo1110.hadoop.service.LogAnalysisService;
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

    @GetMapping("/runLogAnalysis")
    public String logAnalysis(String inputPath, String outputPath) {
        // 假定数据存放在HDFS上的/logs/input目录里
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
        // 假定数据存放在HDFS上的/phoneTraffic/input目录里
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
        // 假定数据存放在HDFS上的/phoneTraffic/input目录里
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
        // 假定数据存放在HDFS上的/phoneTraffic/input目录里
        inputPath = "/phoneTrafficSort/input";
        outputPath = "/phoneTrafficSort/output";
        try {
            this.phoneTrafficSortService.runJob(inputPath, outputPath);
            return "ok";
        } catch (Exception e) {
            return e.getMessage();
        }
    }
}
