
package org.paceWithMe.hadoop.helpers;

import java.io.*;
import java.net.*;
import java.text.*;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.jobcontrol.*;
import org.apache.hadoop.mapreduce.lib.output.*;

import org.apache.hadoop.util.*;
import org.slf4j.*;
import org.paceWithMe.hadoop.helpers.AggregateData;
import org.paceWithMe.hadoop.helpers.AggregateWritable;
import org.paceWithMe.hadoop.helpers.Transaction;

public class MerchantAnalyticsJob extends Configured implements Tool{

	private final static Logger LOGGER = LoggerFactory.getLogger(MerchantAnalyticsJob.class);
	static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

	public static class MerchantPartitioner extends Partitioner<Text,AggregateWritable>{
		@Override
		public int getPartition(Text key,AggregateWritable value,int numPartitions){
			return Math.abs(key.toString().hashCode()) % numPartitions;
		}
	}


	private static class TransactionMapper extends Mapper<LongWritable,Text,Text,AggregateWritable>{

		private static Map<String,String> merchantIdNameMap = new HashMap<String,String>();

		@Override
		protected void setup(Mapper<LongWritable,Text,Text,AggregateWritable>.Context context) throws Exception{
			URI[] paths = context.getCacheArchives();
			if(paths != null){
				for(URI path : paths){
					loadMerchantIdNameInCache(path.toString(),context.getConfiguration());
				}
			}
			super.setup(context);
		}

		private void loadMerchantIdNameInCache(String file,Configuration conf){
			LOGGER.info("file name : " + file);
			String strRead;
			BufferedReader br = null;
			try{
				FileSystem fileSystem = FileSystem.get(conf);
				FSDataInputStream open = fileSystem.open(new Path(file));
				br = new BufferedReader(new InputStreamReader(open));
				while((strRead = br.readLine() != null)){
					String line = strRead.toString().replace("\"","");
					String splitarray[] = line.split(",");
					merchantIdNameMap.put(splitarray[0].toString(),splitarray[2].toString());
				}
			}catch(Exception e){
				LOGGER.error("exception occured while loading data in cache");
			}finally{

				try{
					if(br != null)
						br.close();
				}catch(Exception e){
					LOGGER.error("exception occured while closing the file reader = {}",e);
				}
			}
		}

		@Override
		protected void map(LongWritable key,Text value,Mapper<LongWritable,Text ,Text,AggregateWritable>.Context context) throws Exception{
			String line = value.toString().replace("\"","");

			if(line.indexOf("transaction") != -1){
				return;
			}

			String split[] = line.split(",");
			Transaction transaction = new Transaction();
			transaction.setTxId(split[0]);
			transaction.setCustomerId(Long.parseLong(split[1]));
			transaction.setMerchantId(Long.parseLong(split[2]));
			transaction.setTimestamp(split[3].split(" ")[0].trim());
			transaction.setInvoiceNumber(split[4].split());
			transaction.setInvoiceAmount(Float.parseFloat(split[5]));
			transaction.setSegment(split[6].trim());

			AggregateData aggregatedata = new AggregateData();
			AggregateWritable aggregateWritable = new AggregateWritable(aggregatedata);
			if(transaction.getInvoiceAmount() <= 5000)
				aggregatedata.setOrderBelow5000(1l);
			else if(transaction.getInvoiceAmount() <= 10000)
				aggregatedata.setOrderBelow10000(1l);
			else if(transaction.getInvoiceAmount() <= 20000)
				aggregatedata.setOrderBelow20000(1l);
			else
				aggregatedata.setOrderAbove20000(1l);


			aggregatedata.setTotalOrder(1l);
			String outputkey = merchantIdNameMap.get(transaction.getMerchantId().toString()) + "-" + (split[3].trim().split(" ")[0].trim());
			context.write(new Text(outputkey),aggregateWritable);
		}

	}



	public static class MerchantOrderReducer extends Reducer<Text, AggregateWritable, Text, AggregateWritable>{

		public void reduce(Text key, Iterable<AggregateWritable> values, Context context) throws Exception{
			AggregateData aggregatedata = new AggregateData();
			AggregateWritable aggregateWritable = new AggregateWritable(aggregatedata);
		
			for(AggregateWritable val: values){
				aggregatedata.setOrderBelow20000(aggregatedata.getOrderBelow20000() + val.getAggregateData().getOrderBelow20000());
				aggregatedata.setOrderAbove20000(aggregatedata.getOrderAbove20000() + val.getAggregateData().getOrderAbove20000());
				aggregatedata.setOrderBelow10000(aggregatedata.getOrderBelow10000() + val.getAggregateData().getOrderBelow10000());
				aggregatedata.setOrderBelow5000(aggregatedata.getOrderBelow5000() + val.getAggregateData().getOrderBelow5000());
				aggregatedata.setTotalOrder(aggregatedata.getTotalOrder() + val.getAggregateData().getTotalOrder());
			}

			context.write(key,aggregateWritable);
		}
	}




	public static void main(String[] args){
		ToolRunner.run(new Configuration(),new MerchantAnalyticsJob(),args);
		System.exit();
	}



	public static int runMRJob(String[] args) throws Exception{
		Configuration conf = new Configuration();
		ControlledJob myJob1 = new ControlledJob(conf);
		Job job = myJob1.getJob();

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(AggregateWritable.class);
		job.setJarByClass(MerchantAnalyticsJob.class);

		job.setReduceClass(MerchantOrderReducer.class);
		FileInputFormat.setInputDirRecursive(job,true);
		MultipleInputs.addInputPath(job,new Path[args[0]] , TextInputFormat.class, TransactionMapper.class);
		FileSystem filesystem = FileSystem.get(job.getConfiguration());
		RemoteIterator<LocateFileStatus> files = fileSystem.listFiles(new Path[args[1]],true);
		while(files.hasNext()){
			job.addCacheArchive(files.next().getPath().toUri());
		}

		FileOutputFormat.setOutputPath(job,new Path[args[2]] + "/" + Calendar.getInstance().getTimeInMillis());
		job.setNumReduceTask(5);
		job.setPartitionerClass(MerchantPartitioner.class);
		return job.waitForCompletion(true)?0:1;
	}


	@Override
	public int run(String[] args) throws Exception{
		return runMRJob(args);
	}
}
