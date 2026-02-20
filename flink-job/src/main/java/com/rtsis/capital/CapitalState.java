package com.rtsis.capital;

import java.io.Serializable;
import java.time.Instant;

/**
 * Materialised capital state per (fspId, sector, reportingDate).
 * Matches the capital_state table schema.
 * netCoreCapital is computed server-side (generated column in PG), but we store it here for sink convenience.
 */
public class CapitalState implements Serializable {

    private String  fspId;
    private String  sector;
    private String  reportingDate;

    private double  shareCapitalTotal;
    private double  reservesTotal;
    private double  dividendsPayableTotal;
    private double  deductionsTotal;
    private double  netCoreCapital;

    private Instant lastShareUpdate;
    private Instant lastReservesUpdate;
    private Instant lastDividendsUpdate;
    private Instant lastDeductionsUpdate;
    private Instant lastComputedAt;

    private long    eventCount;

    public CapitalState() {}

    /** Recompute netCoreCapital from current component totals */
    public void recompute() {
        this.netCoreCapital = shareCapitalTotal
                            + reservesTotal
                            - dividendsPayableTotal
                            - deductionsTotal;
        this.lastComputedAt = Instant.now();
    }

    // ─── Getters & Setters ────────────────────────────────────────────────────

    public String getFspId()                        { return fspId; }
    public void setFspId(String v)                  { this.fspId = v; }

    public String getSector()                       { return sector; }
    public void setSector(String v)                 { this.sector = v; }

    public String getReportingDate()                { return reportingDate; }
    public void setReportingDate(String v)          { this.reportingDate = v; }

    public double getShareCapitalTotal()            { return shareCapitalTotal; }
    public void setShareCapitalTotal(double v)      { this.shareCapitalTotal = v; }

    public double getReservesTotal()                { return reservesTotal; }
    public void setReservesTotal(double v)          { this.reservesTotal = v; }

    public double getDividendsPayableTotal()        { return dividendsPayableTotal; }
    public void setDividendsPayableTotal(double v)  { this.dividendsPayableTotal = v; }

    public double getDeductionsTotal()              { return deductionsTotal; }
    public void setDeductionsTotal(double v)        { this.deductionsTotal = v; }

    public double getNetCoreCapital()               { return netCoreCapital; }
    public void setNetCoreCapital(double v)         { this.netCoreCapital = v; }

    public Instant getLastShareUpdate()             { return lastShareUpdate; }
    public void setLastShareUpdate(Instant v)       { this.lastShareUpdate = v; }

    public Instant getLastReservesUpdate()          { return lastReservesUpdate; }
    public void setLastReservesUpdate(Instant v)    { this.lastReservesUpdate = v; }

    public Instant getLastDividendsUpdate()         { return lastDividendsUpdate; }
    public void setLastDividendsUpdate(Instant v)   { this.lastDividendsUpdate = v; }

    public Instant getLastDeductionsUpdate()        { return lastDeductionsUpdate; }
    public void setLastDeductionsUpdate(Instant v)  { this.lastDeductionsUpdate = v; }

    public Instant getLastComputedAt()              { return lastComputedAt; }
    public void setLastComputedAt(Instant v)        { this.lastComputedAt = v; }

    public long getEventCount()                     { return eventCount; }
    public void setEventCount(long v)               { this.eventCount = v; }

    @Override
    public String toString() {
        return String.format(
            "CapitalState{fsp=%s, sector=%s, date=%s, share=%.2f, reserves=%.2f, dividends=%.2f, deductions=%.2f, net=%.2f}",
            fspId, sector, reportingDate, shareCapitalTotal, reservesTotal,
            dividendsPayableTotal, deductionsTotal, netCoreCapital);
    }
}
