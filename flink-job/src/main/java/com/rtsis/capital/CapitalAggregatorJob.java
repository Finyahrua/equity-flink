package com.rtsis.capital;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;

/**
 * Main entry point for the Realtime Capital Position Flink Job.
 *
 * This job:
 * 1. Consumes from 4 Kafka topics (Share, Reserves, Dividends, Deductions)
 * 2. Normalises them into a single stream of CapitalEvents
 * 3. Partition by (fspId, sector, reportingDate)
 * 4. Aggregates statefully to maintain current totals
 * 5. Sinks updated totals into PostgreSQL via upsert
 */
public class CapitalAggregatorJob {

    private static final Logger LOG = LoggerFactory.getLogger(CapitalAggregatorJob.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // ─── Configuration ──────────────────────────────────────────────────
        String kafkaBrokers = System.getenv("KAFKA_BROKERS");
        if (kafkaBrokers == null) kafkaBrokers = "localhost:9092";

        String dbUrl = System.getenv("DB_URL");
        if (dbUrl == null) dbUrl = "jdbc:postgresql://host.docker.internal:5432/capital_db";

        String dbUser = System.getenv("DB_USER");
        if (dbUser == null) dbUser = "postgres";

        String dbPass = System.getenv("DB_PASS");
        if (dbPass == null) dbPass = "krukas";

        LOG.info("Starting Capital Aggregator Job...");
        LOG.info("Kafka: {}, DB: {}", kafkaBrokers, dbUrl);

        // ─── Sources ────────────────────────────────────────────────────────

        KafkaSource<CapitalEvent> shareSource = buildKafkaSource(kafkaBrokers, "share-capital", CapitalEvent.SourceType.SHARE);
        KafkaSource<CapitalEvent> reserveSource = buildKafkaSource(kafkaBrokers, "other-capital", CapitalEvent.SourceType.RESERVE);
        KafkaSource<CapitalEvent> dividendSource = buildKafkaSource(kafkaBrokers, "dividends-payable", CapitalEvent.SourceType.DIVIDEND);
        KafkaSource<CapitalEvent> deductionSource = buildKafkaSource(kafkaBrokers, "core-capital-deductions", CapitalEvent.SourceType.DEDUCTION);

        DataStream<CapitalEvent> shareStream = env.fromSource(shareSource, org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks(), "Share Capital Source");
        DataStream<CapitalEvent> reserveStream = env.fromSource(reserveSource, org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks(), "Reserve Source");
        DataStream<CapitalEvent> dividendStream = env.fromSource(dividendSource, org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks(), "Dividend Source");
        DataStream<CapitalEvent> deductionStream = env.fromSource(deductionSource, org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks(), "Deduction Source");

        // ─── Transformation & Aggregation ───────────────────────────────────

        DataStream<CapitalState> resultStream = shareStream
                .union(reserveStream, dividendStream, deductionStream)
                .keyBy(CapitalEvent::compositeKey)
                .process(new CapitalProcessFunction())
                .name("Capital Stateful Aggregator");

        // ─── PostgreSQL Sink (Upsert) ───────────────────────────────────────

        String upsertSql =
                "INSERT INTO capital_state (" +
                "  fsp_id, sector, reporting_date, " +
                "  share_capital_total, reserves_total, dividends_payable_total, deductions_total, " +
                "  last_share_update, last_reserves_update, last_dividends_update, last_deductions_update, " +
                "  last_computed_at, event_count" +
                ") VALUES (?, ?, CAST(? AS DATE), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                "ON CONFLICT (fsp_id, sector, reporting_date) DO UPDATE SET " +
                "  share_capital_total     = EXCLUDED.share_capital_total, " +
                "  reserves_total          = EXCLUDED.reserves_total, " +
                "  dividends_payable_total = EXCLUDED.dividends_payable_total, " +
                "  deductions_total        = EXCLUDED.deductions_total, " +
                "  last_share_update       = EXCLUDED.last_share_update, " +
                "  last_reserves_update    = EXCLUDED.last_reserves_update, " +
                "  last_dividends_update   = EXCLUDED.last_dividends_update, " +
                "  last_deductions_update  = EXCLUDED.last_deductions_update, " +
                "  last_computed_at        = EXCLUDED.last_computed_at, " +
                "  event_count             = EXCLUDED.event_count";

        resultStream.addSink(JdbcSink.sink(
                upsertSql,
                (ps, state) -> {
                    ps.setString(1, state.getFspId());
                    ps.setString(2, state.getSector());
                    ps.setString(3, state.getReportingDate());
                    ps.setDouble(4, state.getShareCapitalTotal());
                    ps.setDouble(5, state.getReservesTotal());
                    ps.setDouble(6, state.getDividendsPayableTotal());
                    ps.setDouble(7, state.getDeductionsTotal());
                    ps.setTimestamp(8, state.getLastShareUpdate() != null ? Timestamp.from(state.getLastShareUpdate()) : null);
                    ps.setTimestamp(9, state.getLastReservesUpdate() != null ? Timestamp.from(state.getLastReservesUpdate()) : null);
                    ps.setTimestamp(10, state.getLastDividendsUpdate() != null ? Timestamp.from(state.getLastDividendsUpdate()) : null);
                    ps.setTimestamp(11, state.getLastDeductionsUpdate() != null ? Timestamp.from(state.getLastDeductionsUpdate()) : null);
                    ps.setTimestamp(12, Timestamp.from(state.getLastComputedAt()));
                    ps.setLong(13, state.getEventCount());
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(1) // Low latency for demo
                        .withMaxRetries(5)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(dbUrl)
                        .withDriverName("org.postgresql.Driver")
                        .withUsername(dbUser)
                        .withPassword(dbPass)
                        .build()
        )).name("PostgreSQL Upsert Sink");

        env.execute("Realtime Capital Position Aggregator");
    }

    private static KafkaSource<CapitalEvent> buildKafkaSource(String brokers, String topic, CapitalEvent.SourceType type) {
        return KafkaSource.<CapitalEvent>builder()
                .setBootstrapServers(brokers)
                .setTopics(topic)
                .setGroupId("capital-aggregator-group-" + topic)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(new CapitalEventDeserializer(type))
                .build();
    }
}
