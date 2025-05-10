package edu.ch.unibas.dis;

import org.apache.spark.api.java.function.MapGroupsWithStateFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.streaming.GroupState;

import java.util.Iterator;

public class PlayerStatsUpdater
        implements MapGroupsWithStateFunction<String, Event, PlayerState, Row> {

    @Override
    public Row call(String playerName,
                    Iterator<Event> events,
                    GroupState<PlayerState> state) {
        PlayerState st = state.exists() ? state.get() : new PlayerState();
        long currentSecond = 0;
        // accumulate deltas
        while (events.hasNext()) {
            Event e = events.next();
            st.kills += e.getKillsDelta();
            st.deaths += e.getDeathsDelta();
            st.assists += e.getAssistsDelta();
            currentSecond = Math.max(currentSecond, e.getSecond());
        }
        state.update(st);

        float kd = st.deaths == 0
                ? st.kills
                : ((float) st.kills) / st.deaths;

        // build a Row matching PlayerStats entity
        return RowFactory.create(
                playerName,
                currentSecond,
                st.kills,
                st.deaths,
                st.assists,
                kd
        );
    }
}