package com.bruce.flink.streaming.connector.redis.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.Locale;
import java.util.Set;

import static com.bruce.flink.streaming.connector.redis.descriptor.RedisValidator.REDIS_COMMAND;

public class RedisDynamicTableSinkFactory implements DynamicTableSinkFactory {

    public static final String IDENTIFIER = "redis";

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        if (context.getCatalogTable().getOptions().containsKey(REDIS_COMMAND)) {
            context.getCatalogTable().getOptions().put(REDIS_COMMAND, context.getCatalogTable().getOptions().get(REDIS_COMMAND).toUpperCase(Locale.ROOT));
        }
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        ReadableConfig config = helper.getOptions();
        helper.validate();
        validateConfigOptions(config);

        return new RedisDy;
    }

    @Override
    public String factoryIdentifier() {
        return null;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return null;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return null;
    }

    private void validateConfigOptions(ReadableConfig config){

    }
}
