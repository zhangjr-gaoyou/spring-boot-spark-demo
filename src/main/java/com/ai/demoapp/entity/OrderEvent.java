package com.ai.demoapp.entity;



import java.util.Date;

import com.ai.base.spark.entity.InboundEvent;
import com.fasterxml.jackson.annotation.JsonFormat;

public class OrderEvent  extends InboundEvent{

	
	public String getStoreId() {
		return storeId;
	}

	public String getStoreType() {
		return storeType;
	}

	public String getStaffId() {
		return staffId;
	}

	public String getCustomerId() {
		return customerId;
	}

	public String getProductId() {
		return productId;
	}

	public Date getTimestamp() {
		return timestamp;
	}

	public int getProdNum() {
		return prodNum;
	}

	public double getProdFee() {
		return prodFee;
	}

	public OrderEvent(String storeId, String storeType, String staffId, String customerId, String productId,
			Date timestamp, int prodNum, double prodFee) {
		super();
		this.storeId = storeId;
		this.storeType = storeType;
		this.staffId = staffId;
		this.customerId = customerId;
		this.productId = productId;
		this.timestamp = timestamp;
		this.prodNum = prodNum;
		this.prodFee = prodFee;
	}

	private String storeId;
	private String storeType;
	private String staffId;
	private String customerId;
	private String productId;
	@JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd HH:mm:ss", timezone="MST")
	private Date timestamp;
	private int prodNum;
	private double prodFee;
	
	public OrderEvent(){
		
	}
	
	
	

}
