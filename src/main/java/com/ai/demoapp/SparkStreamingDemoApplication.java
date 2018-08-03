package com.ai.demoapp;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.context.annotation.ComponentScan;

import com.ai.base.spark.EnableSparkServer;
import com.ai.base.spark.KafkaStreamBuilder;
import com.ai.base.spark.config.KafkaProperties;
import com.ai.base.spark.config.MongodbProperties;
import com.ai.base.spark.config.SparkProperties;
import com.ai.base.spark.util.MongodbWriter;
import com.ai.demoapp.entity.OrderEvent;
import com.ai.demoapp.entity.TotalProducts;
import com.ai.demoapp.processor.DemoProcessor;
import com.mongodb.BasicDBObject;

import scala.Tuple2;

@EnableSparkServer
@ComponentScan
public class SparkStreamingDemoApplication implements ApplicationRunner {

	private static final Logger logger = Logger.getLogger(SparkStreamingDemoApplication.class);

	@Autowired
	protected KafkaProperties kafkaProperties;

	@Autowired
	protected JavaStreamingContext jssc;

	@Autowired
	protected SparkProperties sparkProperties;

	@Autowired
	private MongodbProperties mongodbProperties;

	public static void main(String[] args) throws Exception {

		SpringApplication.run(SparkStreamingDemoApplication.class, args);

	}

	@Override
	public void run(ApplicationArguments args) throws Exception {

//		JavaDStream<OrderEvent> orderDataStream = (JavaDStream<OrderEvent>) (new KafkaStreamBuilder(jssc,
//				sparkProperties, kafkaProperties)).build();
		
		
		MongodbWriter<TotalProducts> mw = new MongodbWriter<TotalProducts>("com.ai.demoapp.entity.TotalProducts",
				mongodbProperties);

		

		/*
		 * 获取输入流
		 */
		@SuppressWarnings("unchecked")
		JavaDStream<OrderEvent> orderDataStream = (JavaDStream<OrderEvent>) (new KafkaStreamBuilder())
				.setJssc(jssc)
				.setKafkaProperties(kafkaProperties)
				.setSparkProperties(sparkProperties)
				.build();
		
		orderDataStream.cache();
		
		/*
		 * 设置初始化状态
		 */
		List<Tuple2<String, Long>> tps = new ArrayList<Tuple2<String, Long>>();

		 BasicDBObject searchQuery = new BasicDBObject();
		 searchQuery.put("recordDate", new SimpleDateFormat("yyyy-MM-dd").format(new Date()));
		
		List<TotalProducts> prodList = mw.read(searchQuery);
		for (TotalProducts prod : prodList) {

			tps.add(new Tuple2<>(prod.getProductId(), prod.getTotalCount()));

		}

		JavaPairRDD<String, Long> initState = jssc.sparkContext().parallelizePairs(tps);


		
		
		/*
		 * 实时数据处理
		 */

		DemoProcessor demoProcessor = new DemoProcessor();

		JavaDStream<TotalProducts> productsDStream = demoProcessor.process(orderDataStream,initState);

		
		/*
		 * 输出数据保存，这里是保存到mongodb
		 */
	
		mw.save(productsDStream);

		/*
		 *  启动上下文
		 *  
		 */
		jssc.start();
		jssc.awaitTermination();

	}

}
