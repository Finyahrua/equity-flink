package com.rtsis.capital;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

/**
 * Stateful aggregator: keyed by (fspId + ":" + sector + ":" + reportingDate).
 *
 * For each incoming CapitalEvent:
 *  1. Load existing CapitalState from Flink managed state
 *  2. Apply event to the appropriate component total
 *  3. Recompute netCoreCapital
 *  4. Save updated state
 *  5. Emit the updated CapitalState to the downstream sink
 */
public class CapitalProcessFunction
        extends KeyedProcessFunction<String, CapitalEvent, CapitalState> {

    private static final Logger LOG = LoggerFactory.getLogger(CapitalProcessFunction.class);
    private static final long serialVersionUID = 1L;

    private transient ValueState<CapitalState> stateStore;

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<CapitalState> descriptor =
                new ValueStateDescriptor<>("capital-state", CapitalState.class);
        stateStore = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(CapitalEvent event,
                               Context ctx,
                               Collector<CapitalState> out) throws Exception {

        // 1. Load or initialise state
        CapitalState state = stateStore.value();
        if (state == null) {
            state = new CapitalState();
            state.setFspId(event.getFspId());
            state.setSector(event.getSector());
            state.setReportingDate(event.getReportingDate());
            state.setShareCapitalTotal(0.0);
            state.setReservesTotal(0.0);
            state.setDividendsPayableTotal(0.0);
            state.setDeductionsTotal(0.0);
            state.setEventCount(0L);
        }

        // 2. Apply the event
        Instant ingestionTime = event.getIngestionTime();
        switch (event.getSourceType()) {
            case SHARE:
                state.setShareCapitalTotal(state.getShareCapitalTotal() + event.getAmount());
                state.setLastShareUpdate(ingestionTime);
                break;
            case RESERVE:
                state.setReservesTotal(state.getReservesTotal() + event.getAmount());
                state.setLastReservesUpdate(ingestionTime);
                break;
            case DIVIDEND:
                state.setDividendsPayableTotal(state.getDividendsPayableTotal() + event.getAmount());
                state.setLastDividendsUpdate(ingestionTime);
                break;
            case DEDUCTION:
                state.setDeductionsTotal(state.getDeductionsTotal() + event.getAmount());
                state.setLastDeductionsUpdate(ingestionTime);
                break;
            default:
                LOG.warn("Unknown source type: {}", event.getSourceType());
                return;
        }

        // 3. Recompute net capital
        state.setEventCount(state.getEventCount() + 1);
        state.recompute();

        LOG.debug("[{}] {} events processed. Net capital = {:.2f}",
                state.getFspId() + "/" + state.getSector() + "/" + state.getReportingDate(),
                state.getEventCount(),
                state.getNetCoreCapital());

        // 4. Persist Flink managed state
        stateStore.update(state);

        // 5. Emit to JDBC sink
        out.collect(state);
    }
}
