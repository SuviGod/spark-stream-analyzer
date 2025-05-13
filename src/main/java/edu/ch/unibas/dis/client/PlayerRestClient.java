package edu.ch.unibas.dis.client;

import edu.ch.unibas.dis.entity.Player;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;

import java.util.List;

@Service
public class PlayerRestClient {

    private final WebClient webClient;

    @Value("${player.api.url:localhost:8081/api/players}")
    private String playerApiUrl;

    public PlayerRestClient(WebClient.Builder webClientBuilder) {
        this.webClient = webClientBuilder.build();
    }

    /**
     * Fetches player data from the external service.
     * 
     * @return A list of player data as CSV strings
     */
    public List<String> fetchPlayerData(String folderPath) {
        return webClient.get()
                .uri(uriBuilder -> uriBuilder
                        .path(playerApiUrl)
                        .queryParam("folderPath", folderPath)
                        .build())
                .retrieve()
                .bodyToMono(String[].class)
                .map(java.util.Arrays::asList)
                .block();
    }

    /**
     * Parses CSV player data and maps it to Player entities.
     * 
     * @param playerDataList List of player data as CSV strings
     * @return List of Player entities
     */
    public List<Player> parsePlayerData(List<String> playerDataList) {
        return Flux.fromIterable(playerDataList)
                .map(this::mapToPlayer)
                .collectList()
                .block();
    }

    /**
     * Maps a CSV string to a Player entity.
     * 
     * @param csvLine CSV string containing player data
     * @return Player entity
     */
    private Player mapToPlayer(String csvLine) {
        String[] fields = csvLine.split(",");

        Player player = new Player();
        player.setName(fields[0]);
        player.setSteamId(fields[1]);
        player.setTeam(fields[3]); // team name is at index 3

        return player;
    }
}
