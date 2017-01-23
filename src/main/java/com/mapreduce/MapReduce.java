package com.mapreduce;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 * Created by hlib on 15.01.17.
 */
public class MapReduce {
    void mapReduce(String fileName) {
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("Lab5"));
        //JavaRDD<String> jFile = sc.textFile("hdfs://localhost:54310/billings/billings", 1);
        JavaRDD<String> jFile = sc.textFile(fileName);
        JavaPairRDD<String, Double> custToSum = jFile.mapToPair(s ->{
            String[] splitted = s.split(",");
            String customer = splitted[1];
            Double amount = Double.parseDouble(splitted[4]);
            return new Tuple2<>(customer, amount);
        });
        System.out.println("custToSum first" + custToSum.first());
        JavaPairRDD<String, Double> outp = custToSum.reduceByKey((a,b)-> a + b);
        JavaPairRDD<String, Double> swapOutp = outp.mapToPair(x->x.swap()).sortByKey(false).mapToPair(x->x.swap());
        System.out.println(swapOutp.take(100));
    }

    public static void main(String[] args) {
        new MapReduce().mapReduce(args[0]);
    }
}
