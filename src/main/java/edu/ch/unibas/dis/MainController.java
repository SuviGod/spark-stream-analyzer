package edu.ch.unibas.dis;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RequestMapping("/api/stream")
@RestController
public class MainController {

    private final SparkStreamingService sparkService;

    public MainController(SparkStreamingService sparkService) {
        this.sparkService = sparkService;
    }

    @PostMapping("/start")
    public String startStream() {
        if (!sparkService.isRunning()) {
            sparkService.startStreaming();
            return "Streaming started";
        }
        return "Streaming already running";
    }

    @PostMapping("/stop")
    public String stopStream() {
        if (sparkService.isRunning()) {
            sparkService.stopStreaming();
            return "Streaming stopped";
        }
        return "Streaming not running";
    }

    @GetMapping("/status")
    public String getStatus() {
        return sparkService.isRunning() ? "Running" : "Stopped";
    }
}