package edu.ch.unibas.dis;

import jakarta.annotation.PostConstruct;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.streaming.*;
import org.apache.spark.sql.types.StructType;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import scala.Function1;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;


@Service
@Slf4j
public class SparkStreamingService {

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
//                .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_LOCATION)
//                .config("spark.driver.extraJavaOptions", "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED")
                .getOrCreate();
    }

    @Async
    public void startStreaming() {
        if (isRunning) {
            return;
        }
        isRunning = true;


        Dataset<String> raw = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
                .option("subscribe", KAFKA_TOPIC_KILLS)
                .option("startingOffsets", "earliest")
                .option("failOnDataLoss", false)
                .load()
                .selectExpr("CAST(value AS STRING)")
                .as(Encoders.STRING());

        Dataset<Event> events = raw.flatMap((FlatMapFunction<String, Event>) line -> {
            String[] cols = line.split(",", -1);
            long tick = Long.parseLong(cols[1]);
            long sec = tick / 128;
            List<Event> list = new ArrayList<>(3);
            // killer
            String killer = cols[3];
            if (!killer.isEmpty())
                list.add(new Event(killer, sec, 1, 0, 0));
            // victim
            String victim = cols[8];
            if (!victim.isEmpty())
                list.add(new Event(victim, sec, 0, 1, 0));
            // assister
            String assist = cols[12];
            if (!assist.isEmpty())
                list.add(new Event(assist, sec, 0, 0, 1));
            return list.iterator();
        }, Encoders.bean(Event.class));

        // 3) Key by playerName and apply stateful updater
        Encoder<Row> rowEnc = RowEncoder.apply(new StructType()
                .add("playerName", "string")
                .add("second", "long")
                .add("kills", "long")
                .add("deaths", "long")
                .add("assists", "long")
                .add("kdRatio", "float")
        );

        PlayerStatsUpdater updater = new PlayerStatsUpdater();
        Dataset<Row> statsStream = events
                .groupByKey((Function1<Event, String>) Event::getPlayer, Encoders.STRING())
                .mapGroupsWithState(
                        updater,
                        Encoders.bean(PlayerState.class),
                        rowEnc
                );

        // 4) Every 1 second micro-batch, write out via JPA

        try {
            // Store the query reference for stopping later
            query = statsStream
                .writeStream()
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
                        ps.setKdRatio(row.getFloat(5));
                        repository.save(ps);
                    });
                })
                .start();

            query.awaitTermination();
        } catch (TimeoutException | StreamingQueryException e) {
            log.error("Error in streaming query", e);
        }
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

}
