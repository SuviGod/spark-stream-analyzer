package edu.ch.unibas.dis;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.function.MapGroupsWithStateFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.streaming.GroupState;
import java.util.Iterator;

public class PlayerStatsUpdater
        implements MapGroupsWithStateFunction<String, Event, PlayerState, Row> {

    @Override
     public Row call(String playerSteamId,
                    Iterator<Event> events,
                    GroupState<PlayerState> groupState) {
        PlayerState playerState = groupState.exists() ? groupState.get() : new PlayerState();
        long currentSecond = 0;

        long currentRound = 0;

        String name = null;
        while (events.hasNext()) {
            Event event = events.next();
            processEvent(event, playerState);
            currentSecond = Math.max(currentSecond, event.getSecond());
            currentRound = Math.max(currentRound, event.getRound());
            if(StringUtils.isBlank(name)) name = event.getPlayer();
        }
        groupState.update(playerState);

        double kd = playerState.deaths == 0
                ? playerState.kills
                : ((double) playerState.kills) / playerState.deaths;
        double damagePerRound = (double) playerState.damage / currentRound;

        // build a Row matching PlayerStats entity
        return RowFactory.create(
                StringUtils.isBlank(name) ? playerSteamId : name,
                currentSecond,
                playerState.kills,
                playerState.deaths,
                playerState.assists,
                playerState.damage,
                kd,
                damagePerRound
        );
    }

    private void processEvent(Event e, PlayerState state) {
        switch (e.getType()) {
            case "kill":
                state.kills++;
                break;
            case "death":
                state.deaths++;
                break;
            case "assist":
                state.assists++;
                break;
            case "damage":
                state.damage += e.getAmount();
        }
    }
}
