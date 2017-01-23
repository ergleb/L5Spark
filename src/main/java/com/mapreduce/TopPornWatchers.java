package com.mapreduce;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 * Created by hlib on 16.01.17.
 */
public class TopPornWatchers {
    void mapReduce(String fileName) {
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("Lab5"));
        //JavaRDD<String> jFile = sc.textFile("hdfs://localhost:54310/billings/billings", 1);
        JavaRDD<String> jFile = sc.textFile(fileName);
        JavaPairRDD<String, Integer> pornWatchers = jFile.mapToPair(s ->{
            String[] splitted = s.split(",");
            String user = splitted[0];
            Integer time;
            if (splitted[1].toLowerCase().contains("porn")) {
                time = Integer.parseInt(splitted[2]);
            } else {
                time = 0;
            }
            return new Tuple2<>(user, time);
        });
        System.out.println("custToSum first" + pornWatchers.first());
        JavaPairRDD<String, Integer> outp = pornWatchers.reduceByKey((a,b)-> a + b);
        JavaPairRDD<String, Integer> swapOutp = outp.mapToPair(x->x.swap()).sortByKey(false).mapToPair(x->x.swap());
        System.out.println(swapOutp.take(100));
    }

    public static void main(String[] args) {
        new TopPornWatchers().mapReduce(args[0]);
    }
}
