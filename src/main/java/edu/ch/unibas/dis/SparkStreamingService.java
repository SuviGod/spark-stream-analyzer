package edu.ch.unibas.dis;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.util.Time;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.streaming.*;
import org.apache.spark.sql.types.StructType;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import scala.Function1;
import scala.Tuple3;
import scala.runtime.AbstractFunction1;
import scala.Serializable;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;


@Service
@Slf4j
public class SparkStreamingService {

    // Serializable function to extract player from Event
    private static class PlayerExtractor extends AbstractFunction1<Event, String> implements Serializable {
        @Override
        public String apply(Event event) {
            return event.getPlayer();
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


        Dataset<String> raw = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
                .option("subscribe", KAFKA_TOPIC_KILLS)
                .option("startingOffsets", "latest")
                .option("failOnDataLoss", false)
                .load()
                .selectExpr("CAST(value AS STRING)")
                .as(Encoders.STRING());

        Dataset<KillRelatedEvent> events = raw.flatMap((FlatMapFunction<String, KillRelatedEvent>) line -> {
            String[] cols = line.split(",", -1);
            long tick = Long.parseLong(cols[1]);
            long sec = tick / 128;
            List<KillRelatedEvent> list = new ArrayList<>(3);
            // killer
            String killer = cols[3];
            if (!killer.isEmpty())
                list.add(new KillRelatedEvent(killer, "kill", sec));
            // victim
            String victim = cols[7];
            if (!victim.isEmpty())
                list.add(new KillRelatedEvent(victim, "death", sec));
            // assister
            String assist = cols[12];
            if (!assist.isEmpty())
                list.add(new KillRelatedEvent(assist, "assist", sec));
            return list.iterator();
        }, Encoders.bean(KillRelatedEvent.class));

//        Dataset<Row> statsStream = events
//                .groupBy("player")
//                .pivot("type", java.util.Arrays.asList("kill", "death", "assist"))
//                .count()
//                .na().fill(0)
//                .withColumn("kdRatio", functions.when(functions.col("death").equalTo(0), functions.col("kill"))
//                        .otherwise(functions.col("kill").divide(functions.col("death"))))
//                .select(
//                        functions.col("player").as("playerName"),
////                        functions.col("second"),
//                        functions.col("kill").as("kills"),
//                        functions.col("death").as("deaths"),
//                        functions.col("assist").as("assists"),
//                        functions.col("kdRatio")
//                );
        try {
            query = events
                    .groupBy("player", "type")
                    .count()
                    .withColumn("count", functions.col("count").cast("long"))
                    .selectExpr("player", "type", "count")
                    .writeStream()
                    .outputMode("update")
                    .format("console")
                    .start();
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        }
//        try {
//            query = events
//                    .groupBy("player", "type")
//                    .count()
//                    .withColumn("count", functions.col("count").cast("long"))
//                    .selectExpr("player", "type", "count")
//                    .writeStream()
//                    .outputMode("update")
//                    .foreachBatch((batchDF, batchId) -> {
//                        Dataset<Row> pivotedBatch = batchDF
//                                .groupBy("player")
//                                .pivot("type", java.util.Arrays.asList("kill", "death", "assist"))
//                                .agg(functions.sum("count"))
//                                .na().fill(0) // Fill nulls with 0
//                                .withColumn("kdRatio", functions.when(functions.col("death").equalTo(0), functions.col("kill"))
//                                        .otherwise(functions.col("kill").divide(functions.col("death"))))
//                                .select(
//                                        functions.col("player").as("playerName"),
//                                        functions.col("kill").as("kills"),
//                                        functions.col("death").as("deaths"),
//                                        functions.col("assist").as("assists"),
//                                        functions.col("kdRatio")
//                                );
//
//                        // Persist each row
//                        pivotedBatch.collectAsList().forEach(row -> {
//                            PlayerStats ps = new PlayerStats();
//                            ps.setPlayerName(row.getAs("playerName"));
//                            ps.setSecond(Time.now());
//                            ps.setKills(row.getAs("kills"));
//                            ps.setDeaths(row.getAs("deaths"));
//                            ps.setAssists(row.getAs("assists"));
//                            ps.setKdRatio(row.getAs("kdRatio"));
//                            repository.save(ps);
//                        });
//                    })
//                    .option("checkpointLocation", CHECKPOINT_LOCATION)
//                    .start();
//            query.awaitTermination();
//        } catch (TimeoutException | StreamingQueryException e) {
//            throw new RuntimeException(e);
//        }


        // Store the query reference for stopping later
//            query = statsStream.writeStream()
//                    .outputMode("complete")
//                    .trigger(Trigger.ProcessingTime("1 second"))
//                    .foreachBatch((batchDF, batchId) -> {
//                        // collect to driver and save one by one
//                        batchDF.collectAsList().forEach(row -> {
//                            PlayerStats ps = new PlayerStats();
//                            ps.setPlayerName(row.getString(0));
//                            ps.setSecond(row.getLong(1));
//                            ps.setKills(row.getLong(2));
//                            ps.setDeaths(row.getLong(3));
//                            ps.setAssists(row.getLong(4));
//                            ps.setKdRatio(row.getFloat(5));
//                            repository.save(ps);
//                        });
//                    })
//                    .start();
//            query = statsStream
//                    .writeStream()
//                    .outputMode("complete")                                  // full aggregation mode
//                    .trigger(Trigger.ProcessingTime("1 second"))             // 1s micro-batches
//                    .foreachBatch((batchDF, batchId) -> {
//                        // 1) write to console for debugging:
//                        batchDF.show(false);
//
//                        // 2) persist each row:
//                        batchDF.collectAsList().forEach(row -> {
//                            PlayerStats ps = new PlayerStats();
//                            ps.setPlayerName( row.getAs("playerName") );
////                            ps.setSecond(     row.getAs("second") );
//                            ps.setSecond(Time.now() );
//
//                            ps.setKills(      row.getAs("kills") );
//                            ps.setDeaths(     row.getAs("deaths") );
//                            ps.setAssists(    row.getAs("assists") );
//                            ps.setKdRatio(    row.getAs("kdRatio") );
//                            repository.save(ps);
//                        });
//                    })
//                    .option("checkpointLocation", CHECKPOINT_LOCATION)
//                    .start();
//
//            query.awaitTermination();

//            query.awaitTermination();

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
