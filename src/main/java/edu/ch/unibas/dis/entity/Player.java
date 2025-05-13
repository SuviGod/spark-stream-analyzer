package edu.ch.unibas.dis.entity;

import lombok.Data;

import javax.persistence.*;

@Entity
@Table(name = "players")
@Data
public class Player {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY) // Adjust the strategy based on your DB setup.
    private Long id;

    private String name;

    @Column(unique = true)
    private String steamId;

    private String team;
}
