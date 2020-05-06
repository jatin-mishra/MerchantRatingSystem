package org.paceWithMe.hadoop.helpers;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import com.google.gson.Gson;

public class AggregateWritable implements Writable{
	private static Gson gson = new Gson();

	private AggregateData aggregateData = new AggregateData();

	public AggregateWritable(){

	}

	public AggregateWritable(AggregateData aggregateData){
		this.aggregateData = aggregateData;
	}

	public AggregateData getAggregateData(){
		return this.aggregateData;
	}

	public void write(DataOutput out) throws IOException{
		out.writeLong(aggregateData.getOrderBelow5000());
		out.writeLong(aggregateData.getOrderBelow10000());
		out.writeLong(aggregateData.getOrderBelow20000());
		out.writeLong(aggregateData.getOrderAbove20000());
		out.writeLong(aggregateData.getTotalOrder());
	}

	public void readFields(DataInput in) throws IOException{
		aggregateData.setOrderBelow5000(in.readLong());
		aggregateData.setOrderBelow10000(in.readLong());
		aggregateData.setOrderBelow20000(in.readLong());
		aggregateData.setOrderAbove20000(in.readLong());
		aggregateData.setTotalOrder(in.readLong());
	}

	@Override 
	public String toString(){
		return gson.toJson(aggregateData);
	}
}