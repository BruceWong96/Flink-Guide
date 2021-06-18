package com.bruce.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SocketTextStreamFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WordCount {

    static Logger logger = LoggerFactory.getLogger(WordCount.class);


    /**
     * String --> Tuple2<String, Integer>
     *
     * Implements the string tokenizer that splits sentences into words as a user-defined
     * FlatMapFunction. The function takes a line (String) and splits it into multiple pairs in the
     * form of "(word,1)" ({@code Tuple2<String, Integer>}).
     */
    public static final class Tokenizer
            implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // normalize and split the line
            String[] tokens = value.toLowerCase().split("\\W+");

            // emit the pairs
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Tuple2<>(token, 1));
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        //参数解析
        if (args.length != 2) {
            logger.error("Usage: \n");
            logger.error("Please input host and port.");
            return;
        }
        String host = args[0];
        int port = Integer.parseInt(args[1]);

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // source
        DataStream<String> source = env.addSource(new SocketTextStreamFunction(host, port, "\n", 0)).name("Source");

        // transform
        DataStream<Tuple2<String, Integer>> counts =
                // split up the lines in pairs (2-tuples) containing: (word,1)
                source.flatMap(new Tokenizer())
                        // group by the tuple field "0" and sum up tuple field "1"
                        .keyBy(value -> value.f0)
                        .sum(1).name("Transform");

        // sink = log
        // 这里为了方便展示效果，将结果直接输出到 log
        counts.addSink(new WordCountSink()).name("Sink");

        // execute program
        env.execute("WordCount from socket by bruce.");
    }
}
