package org.paceWithMe.hadoop.helpers;


import java.io.Serializable;

public class AggregateData implements Serializable{

	private static final long serialVersionUID = 1L;

	private long orderBelow5000 = 0L;
	private long orderBelow10000 = 0L;
	private long orderBelow20000 = 0L;
	private long orderAbove20000 = 0L;
	private Long totalOrder = 0L;


	public void setOrderBelow5000(long orderBelow5000){
		this.orderBelow5000 = orderBelow5000;
	}

	public long getOrderBelow5000(){
		return this.orderBelow5000;
	}

	public void setOrderBelow10000(long orderBelow10000){
		this.orderBelow10000  = orderBelow10000;
	}

	public long getOrderBelow10000(){
		return this.orderBelow10000;
	}

	public void setOrderBelow20000(orderBelow20000){
		this.orderAbove20000 = orderAbove20000;
	}

	public long getOrderBelow20000(){
		return this.orderAbove20000;
	}

	public void setOrderAbove20000(long orderAbove20000){
		this.orderAbove20000 = orderAbove20000;
	}

	public long getOrderAbove20000(){
		return this.orderAbove20000;
	}

	public long getTotalOrder(){
		return this,totalOrder;
	}

	public void setTotalOrder(long totalOrder){
		this.totalOrder = totalOrder;
	}

}
