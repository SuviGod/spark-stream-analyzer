package edu.ch.unibas.dis;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Event {
    public String player;       // killer, victim or assister
    public long second;         // tick / 128
    public long killsDelta;
    public long deathsDelta;
    public long assistsDelta;
}