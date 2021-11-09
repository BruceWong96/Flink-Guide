package com.bruce.flink.streaming.connector.redis.table;

import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;

public class RedisDynamicTableSink implements DynamicTableSink {

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode changelogMode) {

        return null;
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        return null;
    }

    @Override
    public DynamicTableSink copy() {
        return null;
    }

    @Override
    public String asSummaryString() {
        return null;
    }
}
