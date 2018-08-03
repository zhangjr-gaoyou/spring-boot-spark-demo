package com.ai.demoapp.processor;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;


import com.ai.demoapp.entity.OrderEvent;
import com.ai.demoapp.entity.TotalProducts;

import scala.Tuple2;

public class DemoProcessor {
	private static final Logger logger = Logger.getLogger(DemoProcessor.class);

	public JavaDStream<TotalProducts> process(JavaDStream<OrderEvent> orderDataStream){
		return(process(orderDataStream,null));
	}
	
	public JavaDStream<TotalProducts> process(JavaDStream<OrderEvent> orderDataStream,JavaPairRDD<String, Long> initState) {
	    JavaPairDStream<String, Long> countDStreamPair = 
				orderDataStream
				.mapToPair(
						orderEvent -> new Tuple2<String,Long>(orderEvent.getProductId(),new Long(orderEvent.getProdNum()))
						)
				.reduceByKey((a, b) -> a + b);
	
		
	    JavaMapWithStateDStream<String, Long, Long, Tuple2<String, Long>> countDStreamWithStatePair;
		
		      // Need to keep state for total count
	           if(null != initState)
			       countDStreamWithStatePair = countDStreamPair
						.mapWithState(StateSpec
								.function(totalSumFunc)
								.timeout(Durations.seconds(3600*24))
								.initialState(initState)
								);//maintain state for one day
	           else
	        	   countDStreamWithStatePair = countDStreamPair
					.mapWithState(StateSpec
							.function(totalSumFunc)
							.timeout(Durations.seconds(3600*24))
							);//maintain state for one day
	        	   
				// Transform to dstream of ProductData
				JavaDStream<Tuple2<String, Long>> countDStream = countDStreamWithStatePair.map(tuple2 -> tuple2);
				JavaDStream<TotalProducts> productsDStream = countDStream.map(totalProductDataFunc);

				
				return productsDStream;

							
	}
	

	    
	  //Function to get running sum by maintaining the state
	  //?: restartup will lead to sum be reset to 0
	  		private static final Function3<String, Optional<Long>, State<Long>,Tuple2<String,Long>> 
	  		 totalSumFunc = (key,currentSum,state) -> {
	  	        long totalSum = currentSum.or(0L) + (state.exists() ? state.get() : 0);
	  	        Tuple2<String, Long> total = new Tuple2<>(key, totalSum);
	  	        state.update(totalSum);
	  	        return total;
	  	    };
	  	    
	    //Function to create TotalTrafficData object from OrderEvent
	    private static final Function<Tuple2<String, Long>, TotalProducts> totalProductDataFunc = (tuple -> {
			logger.debug("Total Count : " + "key " + tuple._1() + "-" + " value "+ tuple._2());
			TotalProducts productData = new TotalProducts();
			productData.setProductId(tuple._1());
			productData.setTotalCount(tuple._2());
			productData.setTimeStamp(new Date());
			productData.setRecordDate(new SimpleDateFormat("yyyy-MM-dd").format(new Date()));
			return productData;
		});   
	    
}
