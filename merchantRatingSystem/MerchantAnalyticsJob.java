
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

		@Override
		protected void map(LongWritable key,Text value,Mapper<LongWritable,Text ,Text,AggregateWritable>.Context context) throws Exception{

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