package edu.ch.unibas.dis.model;

import lombok.Data;

@Data
public class PlayerState {
    public long kills;
    public long deaths;
    public long assists;
    public long damage;
}
