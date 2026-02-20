package com.rtsis.capital;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.rtsis.capital.CapitalEvent.SourceType;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.Iterator;

/**
 * Generic deserializer that converts Kafka records from any of the 4 capital topics
 * into normalised CapitalEvent objects.
 *
 * The sourceType is passed at construction time so a single class handles all topics.
 */
public class CapitalEventDeserializer
        implements KafkaRecordDeserializationSchema<CapitalEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(CapitalEventDeserializer.class);
    private static final long serialVersionUID = 1L;

    private final CapitalEvent.SourceType sourceType;
    private transient ObjectMapper mapper;

    public CapitalEventDeserializer(CapitalEvent.SourceType sourceType) {
        this.sourceType = sourceType;
    }

    private ObjectMapper getMapper() {
        if (mapper == null) {
            mapper = new ObjectMapper();
            mapper.registerModule(new JavaTimeModule());
        }
        return mapper;
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record,
                            org.apache.flink.util.Collector<CapitalEvent> out) {
        try {
            JsonNode root = getMapper().readTree(record.value());

            // Extract envelope fields
            String fspId  = root.path("fspInformationId").asText("UNKNOWN");
            Instant now   = Instant.now();

            // Extract data array (field name differs per API)
            JsonNode dataArray = getDataArray(root);
            if (dataArray == null || !dataArray.isArray()) {
                LOG.warn("No data array found in payload for fspId={}", fspId);
                return;
            }

            for (JsonNode item : dataArray) {
                String reportingDate  = item.path("reportingDate").asText();
                String transactionDate = item.path("transactionDate").asText(reportingDate);
                String sector = extractSector(item, sourceType);
                double tzsAmount = item.path("tzsAmount").asDouble(0.0);

                CapitalEvent event = new CapitalEvent(
                        sourceType,
                        fspId,
                        sector,
                        reportingDate,
                        transactionDate,
                        tzsAmount,
                        now
                );
                out.collect(event);
            }

        } catch (Exception e) {
            LOG.error("Failed to deserialize record from topic={} sourceType={}: {}",
                    record.topic(), sourceType, e.getMessage(), e);
        }
    }

    private JsonNode getDataArray(JsonNode root) {
        // Try all possible field names
        String[] candidates = {
            "shareCapitalData",
            "otherCapitalAccountData",
            "dividendsPayableData",
            "coreCapitalDeductionsData"
        };
        for (String candidate : candidates) {
            JsonNode node = root.get(candidate);
            if (node != null && node.isArray()) return node;
        }
        return null;
    }

    /**
     * Extracts the sector from the data item.
     * For Share Capital, it's sectorSnaClassification.
     * For other APIs, it may not be present — fall back to a header or default.
     */
    private String extractSector(JsonNode item, SourceType type) {
        // Share Capital carries sector in sectorSnaClassification
        if (type == SourceType.SHARE) {
            String s = item.path("sectorSnaClassification").asText("");
            if (!s.isEmpty()) return s;
        }
        // reserveCategory can indicate sector for reserves (simplified)
        if (type == SourceType.RESERVE) {
            String rc = item.path("reserveCategory").asText("GENERAL");
            // Map reserve category to a sector approximation
            return "GENERAL";
        }
        return "GENERAL";
    }

    @Override
    public TypeInformation<CapitalEvent> getProducedType() {
        return TypeInformation.of(CapitalEvent.class);
    }
}
