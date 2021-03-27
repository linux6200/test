package com.dsm.flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.text.SimpleDateFormat;
import java.util.Date;

public class IotDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool params = ParameterTool.fromArgs(args);

        env.getConfig().setGlobalJobParameters(params);

        DataStream<String> dataStream;

        if(params.has("input") && params.has("agg")) {
            System.out.println("Executing GreenHouse IoT example with file input");
            dataStream = env.readTextFile(params.get("input"));
        } else {
            System.out.println("Use --input to specify file input and --agg for aggregation level");
            System.exit(1);
            return;
        }

        DataStream<String[]> iotDataStream = dataStream.filter(new IotDemo.FilterHeader())
                .map(new IotDemo.Splitter());

        DataStream<Tuple2<String, Double>> result = null;
        String aggLevel = params.get("agg");
        if ("day".equals(aggLevel)) {
            result = iotDataStream.map(new DateTuple())
                    .keyBy(0)
                    .sum(1);
        } else if ("hour".equals(aggLevel)) {
            result = iotDataStream.map(new HourTuple())
                    .keyBy(0)
                    .sum(1);
        } else if ("minute".equals(aggLevel)) {
            result = iotDataStream.map(new MinuteTuple())
                    .keyBy(0)
                    .sum(1);
        } else {
            System.out.println();
        }

        env.execute("GreenHouse IoT");
        result.print();
    }

    public static class FilterHeader implements FilterFunction<String> {
        public boolean filter(String input) throws Exception {
            try {
                return !input.contains("Date Time");
            } catch (Exception ex) {
            }
            return false;
        }
    }

    public static class Splitter implements MapFunction<String, String[]> {
        public String[] map(String sentence) throws Exception {
            return sentence.split(",");
        }
    }

    public static class DateTuple implements MapFunction<String[], Tuple2<String, Double>> {
        public Tuple2<String, Double> map(String input[]) throws Exception {
            double elecCons = Double.parseDouble(input[1].trim());
            Date date = new Date( Long.parseLong(input[0]) * 1000 );
            return new Tuple2<String, Double>(new SimpleDateFormat("yyyy-MM-dd").format(date), elecCons);
        }
    }

    public static class HourTuple implements MapFunction<String[], Tuple2<String, Double>> {
        public Tuple2<String, Double> map(String input[]) throws Exception {
            double elecCons = Double.parseDouble(input[1].trim());
            Date date = new Date( Long.parseLong(input[0]) * 1000 );
            return new Tuple2<String, Double>(new SimpleDateFormat("yyyy-MM-dd HH").format(date), elecCons);
        }
    }

    public static class MinuteTuple implements MapFunction<String[], Tuple2<String, Double>> {
        public Tuple2<String, Double> map(String input[]) throws Exception {
            double elecCons = Double.parseDouble(input[1].trim());
            Date date = new Date( Long.parseLong(input[0]) * 1000 );
            return new Tuple2<String, Double>(new SimpleDateFormat("yyyy-MM-dd HH:mm").format(date), elecCons);
        }
    }

}
