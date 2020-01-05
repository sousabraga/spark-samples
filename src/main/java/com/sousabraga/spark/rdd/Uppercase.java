package com.sousabraga.spark.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Uppercase {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("uppercase").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("in/uppercase.text");
        JavaRDD<String> upperCaseLines = lines.map(line -> line.toUpperCase());

        upperCaseLines.saveAsTextFile("out/uppercase.text");
        
        sc.close();
	}
	
}
