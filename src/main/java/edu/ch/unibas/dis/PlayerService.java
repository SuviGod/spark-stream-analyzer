package edu.ch.unibas.dis;

import edu.ch.unibas.dis.client.PlayerRestClient;
import edu.ch.unibas.dis.entity.Player;
import edu.ch.unibas.dis.repository.PlayerRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class PlayerService {

    private static final Logger logger = LoggerFactory.getLogger(PlayerService.class);

    private final PlayerRepository playerRepository;
    private final PlayerRestClient playerRestClient;

    public PlayerService(PlayerRepository playerRepository, PlayerRestClient playerRestClient) {
        this.playerRepository = playerRepository;
        this.playerRestClient = playerRestClient;
    }

    /**
     * Fetches player data from the external service and saves it to the database.
     * 
     * @return The number of players saved
     */
    public int fetchAndSavePlayers(String folderPath) {
        try {
            List<String> playerDataList = playerRestClient.fetchPlayerData(folderPath);
            List<Player> players = playerRestClient.parsePlayerData(playerDataList);

            logger.info("Fetched {} players from external service", players.size());

            playerRepository.deleteAll();
            List<Player> savedPlayers = playerRepository.saveAll(players);

            logger.info("Saved {} players to database", savedPlayers.size());

            return savedPlayers.size();
        } catch (Exception e) {
            logger.error("Error fetching and saving players", e);
            throw new RuntimeException("Failed to fetch and save players", e);
        }
    }

    /**
     * Gets all players from the database.
     * 
     * @return List of all players
     */
    public List<Player> getAllPlayers() {
        return playerRepository.findAll();
    }
}
