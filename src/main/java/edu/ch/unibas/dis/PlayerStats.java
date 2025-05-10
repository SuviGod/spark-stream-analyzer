package edu.ch.unibas.dis;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Data;

@Entity
@Table(name = "player_stats")
@Data
public class PlayerStats {
    @Id
    private String playerName;
    private long second;
    private long kills;
    private long deaths;
    private long assists;
    private float kdRatio;

}