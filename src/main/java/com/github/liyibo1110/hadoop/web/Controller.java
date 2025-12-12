package com.github.liyibo1110.hadoop.web;

import com.github.liyibo1110.hadoop.service.LogAnalysisService;
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

    @GetMapping("/runLogAnalysis")
    public String logAnalysis(String inputPath, String outputPath) {
        // 假定日志数据存放在HDFS上的/logs/input目录里
        inputPath = "/logAnalyzer/input";
        outputPath = "/logAnalyzer/output";
        try {
            this.logAnalysisService.runJob(inputPath, outputPath);
            return "ok";
        } catch (Exception e) {
            return e.getMessage();
        }
    }

    @GetMapping("/test")
    public String test() {
        return "ok";
    }
}
