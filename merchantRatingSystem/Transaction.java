package org.paceWithMe.hadoop.helpers;

import java.io.Serializable;

public class Transacttion implements Serializable{
	private static final long serialVersionId = 1L;

	private String txId;
	private Long customerId;
	private Long merchantId;
	private String timeStamp;
	private String invoiceNumber;
	private float invoiceAmount;
	private String segment;

	public String getSegment(){
		return this.segment;
	}

	public void setSegment(String segment){
		this.segment = segment;
	}


	public String getTxId(){
		return this.txId;
	}

	public void setTxId(String txId){
		this.txId = txId;
	}


	public Long getCustomerId(){
		return this.customerId;
	}

	public void setCustomerId(Long customerId){
		this.customerId = customerId;
	}

	public Long getMerchantId(){
		return this.merchantId;
	}

	public void setMerchantId(Long merchantId){
		this.merchantId = merchantId;
	}

	public String getTimeStamp(){
		return this.timeStamp;
	}

	public void setTimeStamp(String timeStamp){
		this.timeStamp = timeStamp;
	}

	public String getInvoiceNumber(){
		return this.invoiceNumber;
	}

	public void setInvoiceNumber(String invoiceNumber){
		this.invoiceNumber = invoiceNumber;
	}

	public float getInvoiceAmount(){
		return this.invoiceAmount;
	}


	public void setInvoiceAmount(float invoiceAmount){
		this.invoiceAmount = invoiceAmount;
	}
}