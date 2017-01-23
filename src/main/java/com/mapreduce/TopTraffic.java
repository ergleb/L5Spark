package com.mapreduce;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 * Created by hlib on 16.01.17.
 */
public class TopTraffic {
    void mapReduce(String fileName) {
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("Lab5"));
        //JavaRDD<String> jFile = sc.textFile("hdfs://localhost:54310/billings/billings", 1);
        JavaRDD<String> jFile = sc.textFile(fileName);
        JavaPairRDD<String, Integer> siteToTraffic= jFile.mapToPair(s ->{
            String[] splitted = s.split(",");
            String website = splitted[1];
            Integer traffic = Integer.parseInt(splitted[3]);
            return new Tuple2<>(website, traffic);
        });
        System.out.println("custToSum first" + siteToTraffic.first());
        JavaPairRDD<String, Integer> outp = siteToTraffic.reduceByKey((a,b)-> a + b);
        JavaPairRDD<String, Integer> swapOutp = outp.mapToPair(x->x.swap()).sortByKey(false).mapToPair(x->x.swap());
        System.out.println(swapOutp.take(100));
    }

    public static void main(String[] args) {
        new TopTraffic().mapReduce(args[0]);
    }
}
