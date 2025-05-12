package edu.ch.unibas.dis;

import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.streaming.*;
import org.apache.spark.sql.types.StructType;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import scala.Function1;
import scala.runtime.AbstractFunction1;
import scala.Serializable;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeoutException;


@Service
@Slf4j
public class SparkStreamingService {

    // Serializable function to extract player from Event
    private static class PlayerExtractor extends AbstractFunction1<Event, String> implements Serializable {
        @Override
        public String apply(Event event) {
            return event.getSteamId();
        }
    }

    @Value("${spark.streaming.checkpoint-location}")
    private String CHECKPOINT_LOCATION;

    @Value("${kafka.bootstrap-servers}")
    private String KAFKA_BOOTSTRAP_SERVERS;

    @Value("${kafka.topic.kills}")
    private String KAFKA_TOPIC_KILLS;

    @Value("${kafka.topic.damages}")
    private String KAFKA_TOPIC_DAMAGES;

    private SparkSession spark;
    private StreamingQuery query;

    @Getter
    private boolean isRunning = false;

//    @Autowired
    private final PlayerStatsRepository repository;

    public SparkStreamingService(PlayerStatsRepository repository) {

        this.repository = repository;
    }

    @PostConstruct
    public void init() {
        spark = SparkSession.builder()
                .appName("CS Stats Streaming")
                .master("local[*]")
                .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_LOCATION)
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
    }

    @Async
    public void startStreaming() {
        if (isRunning) {
            return;
        }
        isRunning = true;

        Dataset<Event> events = getKillRelatedEvents().union(getDamageRelatedEvents());
        // 3) Key by playerName and apply stateful updater
        Encoder<Row> rowEnc = RowEncoder.apply(new StructType()
                .add("playerName", "string")
                .add("second", "long")
                .add("kills", "long")
                .add("deaths", "long")
                .add("assists", "long")
                .add("damage", "long")
                .add("kdRatio", "double")
                .add("damagePerRound", "double")
        );

        PlayerStatsUpdater updater = new PlayerStatsUpdater();
        Dataset<Row> statsStream = events
                .groupByKey(new PlayerExtractor(), Encoders.STRING())
                .mapGroupsWithState(
                        updater,
                        Encoders.bean(PlayerState.class),
                        rowEnc
                );

        simpleOutput(statsStream);
    }

    public void stopStreaming() {
        if (query != null && isRunning) {
            try {
                query.stop();
                repository.deleteAll();
            } catch (Exception e) {
                log.error("Error stopping streaming query", e);
            }
            isRunning = false;
        }
    }

    private Dataset<Event> getKillRelatedEvents() {
        Dataset<String> raw = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
                .option("subscribe", KAFKA_TOPIC_KILLS)
                .option("startingOffsets", "latest")
                .option("failOnDataLoss", false)
                .load()
                .selectExpr("CAST(value AS STRING)")
                .as(Encoders.STRING());

        return  raw.flatMap((FlatMapFunction<String, Event>) line -> {
            String[] cols = line.split(",", -1);
            long tick = Long.parseLong(cols[1]);
            long sec = tick / 128;

            long round = Long.parseLong(cols[2]);
            List<Event> list = new ArrayList<>(3);
            // killer
            String killer = cols[3];
            String killer_id = cols[4];
            if (!killer.isEmpty()) {
                list.add(new Event(
                        killer,
                        killer_id,
                        "kill",
                        sec,
                        0,
                        round));
            }

            String victim = cols[7];
            String victim_id = cols[8];
            if (!victim.isEmpty()) {
                list.add(new Event(
                        victim,
                        victim_id,
                        "death",
                        sec,
                        0,
                        round));
            }

            String assist = cols[11];
            String assist_id = cols[12];
            if (!assist.isEmpty() && !assist.equals("0")) {
                list.add(new Event(
                        assist,
                        assist_id,
                        "assist",
                        sec,
                        0,
                        round));
            }
            return list.iterator();
        }, Encoders.bean(Event.class));
    }

    private Dataset<Event> getDamageRelatedEvents() {
        Dataset<String> raw = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
                .option("subscribe", KAFKA_TOPIC_DAMAGES)
                .option("startingOffsets", "latest")
                .option("failOnDataLoss", false)
                .load()
                .selectExpr("CAST(value AS STRING)")
                .as(Encoders.STRING());

        return  raw.map((MapFunction<String, Event>) line -> {
            String[] cols = line.split(",", -1);
            long tick = Long.parseLong(cols[1]);
            long sec = tick / 128;

            long round = Long.parseLong(cols[2]);
            long old_hp = Long.parseLong(cols[5]);
            long new_hp = Long.parseLong(cols[6]);
            // killer
            String damager_id = cols[9];
            if (!damager_id.isEmpty()) {
                return new Event(
                        "",
                        damager_id,
                        "damage",
                        sec,
                        old_hp-new_hp,
                        round);
            }
            return null;
        }, Encoders.bean(Event.class))
                .filter((FilterFunction<Event>) Objects::nonNull);
    }


    private void simpleOutput(Dataset<Row> datasetToOutput){
        try {
            query = datasetToOutput
            .writeStream()
            .outputMode("update")
            .format("console")
            .start();
            query.awaitTermination();
        } catch (StreamingQueryException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }


    private void outputToDatabase(Dataset<Row> datasetToOutput){
        try {
            query = datasetToOutput
                    .writeStream()
                    .outputMode("update")
                    .trigger(Trigger.ProcessingTime("1 second"))
                    .foreachBatch((batchDF, batchId) -> {
                        // collect to driver and save one by one
                        batchDF.collectAsList().forEach(row -> {
                            PlayerStats ps = new PlayerStats();
                            ps.setPlayerName(row.getString(0));
                            ps.setSecond(row.getLong(1));
                            ps.setKills(row.getLong(2));
                            ps.setDeaths(row.getLong(3));
                            ps.setAssists(row.getLong(4));
                            ps.setDamage(row.getLong(5));
                            ps.setKdRatio(row.getDouble(6));
                            ps.setDamagePerRound(row.getDouble(7));
                            repository.save(ps);
                        });
                    })
                    .start();

            query.awaitTermination();

        } catch (TimeoutException e) {
            log.error("Error in streaming query", e);
        } catch (StreamingQueryException e) {
            throw new RuntimeException(e);
        }
    }


}
