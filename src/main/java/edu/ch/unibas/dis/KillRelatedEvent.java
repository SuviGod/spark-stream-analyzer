package edu.ch.unibas.dis;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class KillRelatedEvent {

    private String player;

    private String type;

    private long second;
}
