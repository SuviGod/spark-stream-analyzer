package edu.ch.unibas.dis;

import edu.ch.unibas.dis.client.KafkaStreamingClient;
import edu.ch.unibas.dis.entity.Player;
import edu.ch.unibas.dis.spark.SparkStreamingService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api")
public class MainController {

    private final SparkStreamingService sparkService;
    private final PlayerService playerService;
    private final KafkaStreamingClient kafkaStreamingClient;

    @Value("${data.default.directory}")
    private String folderPath;

    public MainController(SparkStreamingService sparkService, PlayerService playerService, KafkaStreamingClient kafkaStreamingClient) {
        this.sparkService = sparkService;
        this.playerService = playerService;
        this.kafkaStreamingClient = kafkaStreamingClient;
    }

    @PostMapping("/stream/start")
    public String startStream(@RequestParam(required = false) String folderPath) {
        if (!sparkService.isRunning()) {
            // Create a final copy of the folder path
            final String inputFolder = (folderPath == null) ? this.folderPath : folderPath;

            playerService.fetchAndSavePlayers(inputFolder);

            sparkService.startStreaming();
            String response = kafkaStreamingClient.startStreaming(inputFolder);

            return "Streaming started. Kafka response: " + response;
        }
        return "Streaming already running";
    }

    @PostMapping("/stream/stop")
    public String stopStream() {
        if (sparkService.isRunning()) {
            // Call Kafka streaming application to stop streaming
            String response = kafkaStreamingClient.stopStreaming();

            sparkService.stopStreaming();

            return "Streaming stopped. Kafka response: " + response;
        }
        return "Streaming not running";
    }

    @GetMapping("/status")
    public String getStatus() {
        return sparkService.isRunning() ? "Running" : "Stopped";
    }
}
