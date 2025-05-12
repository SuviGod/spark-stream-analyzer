package edu.ch.unibas.dis;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Event {

    private String player;

    private String steamId;

    private String type;

    private long second;

    private long amount;

    private long round;

}
