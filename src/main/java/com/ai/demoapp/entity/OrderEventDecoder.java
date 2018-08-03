package com.ai.demoapp.entity;



import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;

import com.fasterxml.jackson.databind.ObjectMapper;




public class OrderEventDecoder implements Deserializer<OrderEvent> {
	
	private static ObjectMapper objectMapper = new ObjectMapper();
	

	
	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void configure(Map<String, ?> arg0, boolean arg1) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public OrderEvent deserialize(String topic, byte[] bytes) {
		try {
			return objectMapper.readValue(bytes, OrderEvent.class);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	
	}

}

