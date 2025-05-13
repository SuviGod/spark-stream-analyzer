package edu.ch.unibas.dis.repository;

import edu.ch.unibas.dis.entity.Player;
import org.springframework.data.jpa.repository.JpaRepository;

public interface PlayerRepository extends JpaRepository<Player, Long> {

    Player findBySteamId(String steamId);

}
