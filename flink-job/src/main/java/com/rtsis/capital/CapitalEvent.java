package com.rtsis.capital;

import java.io.Serializable;
import java.time.Instant;

/**
 * Canonical event model.
 * Every incoming Kafka record from any of the 4 APIs is normalised into this form.
 */
public class CapitalEvent implements Serializable {

    public enum SourceType { SHARE, RESERVE, DIVIDEND, DEDUCTION }

    private SourceType sourceType;
    private String     fspId;
    private String     sector;
    private String     reportingDate;   // ISO date string yyyy-MM-dd
    private String     transactionDate;
    private double     amount;          // tzsAmount (always normalised to TZS)
    private Instant    ingestionTime;

    public CapitalEvent() {}

    public CapitalEvent(SourceType sourceType, String fspId, String sector,
                        String reportingDate, String transactionDate,
                        double amount, Instant ingestionTime) {
        this.sourceType     = sourceType;
        this.fspId          = fspId;
        this.sector         = sector;
        this.reportingDate  = reportingDate;
        this.transactionDate = transactionDate;
        this.amount         = amount;
        this.ingestionTime  = ingestionTime;
    }

    // ─── Getters & Setters ────────────────────────────────────────────────────

    public SourceType getSourceType()           { return sourceType; }
    public void setSourceType(SourceType v)     { this.sourceType = v; }

    public String getFspId()                    { return fspId; }
    public void setFspId(String v)              { this.fspId = v; }

    public String getSector()                   { return sector; }
    public void setSector(String v)             { this.sector = v; }

    public String getReportingDate()            { return reportingDate; }
    public void setReportingDate(String v)      { this.reportingDate = v; }

    public String getTransactionDate()          { return transactionDate; }
    public void setTransactionDate(String v)    { this.transactionDate = v; }

    public double getAmount()                   { return amount; }
    public void setAmount(double v)             { this.amount = v; }

    public Instant getIngestionTime()           { return ingestionTime; }
    public void setIngestionTime(Instant v)     { this.ingestionTime = v; }

    /** Composite key used for Flink keyBy partitioning */
    public String compositeKey() {
        return fspId + ":" + sector + ":" + reportingDate;
    }

    @Override
    public String toString() {
        return String.format("CapitalEvent{type=%s, fsp=%s, sector=%s, date=%s, amount=%.2f}",
                sourceType, fspId, sector, reportingDate, amount);
    }
}
