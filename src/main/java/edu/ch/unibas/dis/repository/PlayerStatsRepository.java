package edu.ch.unibas.dis.repository;

import edu.ch.unibas.dis.entity.PlayerStats;
import org.springframework.data.jpa.repository.JpaRepository;

public interface PlayerStatsRepository extends JpaRepository<PlayerStats, Long> {

}
