package com.sousabraga.spark.rdd;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class AirportsByLatitude {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("airports").setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        JavaRDD<String> airports = sc.textFile("in/airports.text");
        JavaRDD<String> airportsInUSA = airports.filter(line -> Float.valueOf(line.split(Utils.COMMA_DELIMITER)[6]) > 40);
        JavaRDD<String> airportsNameAndCityNames = airportsInUSA
    		.map(line -> {
                String[] splits = line.split(Utils.COMMA_DELIMITER);
                return StringUtils.join(new String[]{splits[1], splits[6]}, ",");
            }
        );
        
        airportsNameAndCityNames.saveAsTextFile("out/airports_by_latitude.text");
        
        sc.close();
	}
	
}
