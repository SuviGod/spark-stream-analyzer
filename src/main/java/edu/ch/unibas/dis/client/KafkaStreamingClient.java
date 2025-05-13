package edu.ch.unibas.dis.client;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

@Service
public class KafkaStreamingClient {

    private final WebClient webClient;

    @Value("${kafka.streaming.url:localhost:8081/api/streaming}")
    private String kafkaStreamingUrl;

    public KafkaStreamingClient(WebClient.Builder webClientBuilder) {
        this.webClient = webClientBuilder.build();
    }

    /**
     * Starts the Kafka streaming application.
     *
     * @param inputFolder The folder path to read data from
     * @return The response from the Kafka streaming application
     */
    public String startStreaming(String inputFolder) {
        return webClient.post()
            .uri(uriBuilder -> uriBuilder
                .path(kafkaStreamingUrl + "/start")
                .queryParam("inputFolder", inputFolder)
                .build())
            .contentType(org.springframework.http.MediaType.APPLICATION_JSON)
            .retrieve()
            .bodyToMono(String.class)
            .block();
    }

    /**
     * Stops the Kafka streaming application.
     *
     * @return The response from the Kafka streaming application
     */
    public String stopStreaming() {
        return webClient.post()
            .uri(kafkaStreamingUrl + "/stop")
            .contentType(org.springframework.http.MediaType.APPLICATION_JSON)
            .retrieve()
            .bodyToMono(String.class)
            .block();
    }
}