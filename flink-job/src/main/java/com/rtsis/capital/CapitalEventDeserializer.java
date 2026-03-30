package com.rtsis.capital;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

/**
 * Flink Kafka deserializer for CapitalEvent
 *
 * This class handles deserialization from all 4 capital topics:
 * Share, Reserve, Dividend, Deduction.
 */
public class CapitalEventDeserializer implements KafkaRecordDeserializationSchema<CapitalEvent> {

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
                            Collector<CapitalEvent> out) {
        try {
            JsonNode root = getMapper().readTree(record.value());

            String fspId = root.path("fspInformationId").asText("UNKNOWN");
            Instant now = Instant.now();

            JsonNode dataArray = getDataArray(root);
            if (dataArray == null || !dataArray.isArray()) {
                LOG.warn("No data array found in payload for fspId={}", fspId);
                return;
            }

            for (JsonNode item : dataArray) {
                String reportingDate = item.path("reportingDate").asText();
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

    /**
     * Tries all possible data array field names to extract array from payload.
     */
    private JsonNode getDataArray(JsonNode root) {
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
     * Extract sector from data item.
     * Share: use sectorSnaClassification
     * Reserve: default to GENERAL
     * Dividend / Deduction: default to GENERAL
     */
    private String extractSector(JsonNode item, CapitalEvent.SourceType type) {
        if (type == CapitalEvent.SourceType.SHARE) {
            String s = item.path("sectorSnaClassification").asText("");
            if (!s.isEmpty()) return s;
        }
        if (type == CapitalEvent.SourceType.RESERVE) {
            return "GENERAL";
        }
        return "GENERAL";
    }

    @Override
    public org.apache.flink.api.common.typeinfo.TypeInformation<CapitalEvent> getProducedType() {
        return org.apache.flink.api.common.typeinfo.TypeInformation.of(CapitalEvent.class);
    }
}