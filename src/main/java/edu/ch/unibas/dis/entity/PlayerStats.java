package edu.ch.unibas.dis.entity;

import lombok.Data;

import javax.persistence.*;

@Entity
@Table(name = "player_stats")
@Data
public class PlayerStats {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY) // Adjust the strategy based on your DB setup.
    private Long id;

    private String playerName;
    private long second;
    private long kills;
    private long deaths;
    private long assists;
    private double kdRatio;
    private double damagePerRound;
    private long damage;

}
