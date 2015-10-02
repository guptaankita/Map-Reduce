package com.trial;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class SongCounter {

	public static class Map extends Mapper<Object, Text, Text, Text> {
		
		String[] tokens = null;
		String[] userSongs = null;
		//this is used to aggregate songs and its users.
		HashMap<String, String> map;
		
		@Override
		protected void setup(Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			map = new HashMap<String,String>();
			super.setup(context);
		}
		
		// map function
		@Override
		public void map(Object key, Text line, Context context)
				throws IOException, InterruptedException {
			String input = line.toString();
			if(input.length() > 0){
				userSongs = input.split("\t");
				tokens = userSongs[2].split(",");
				for(int i=0; i< tokens.length; i++)
				{
					if(map.containsKey(tokens[i]))
					{
						map.put(tokens[i], map.get(tokens[i]) + "#" + userSongs[1]);
					}
					else
					{
						map.put(tokens[i], userSongs[1]);
					}
				}
			}
		}
		
		@Override
		protected void cleanup(Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			Set<String> pairs = map.keySet();
			Text outKey = new Text();
			Text outVal = new Text();
			String temp;
			for(String key : pairs)
			{
				temp = map.get(key);
				outKey.set(key);
				outVal.set(temp);
				context.write(outKey, outVal);
			}
			
			super.cleanup(context);
		}

	} // end of mapper

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			Iterator<Text> it = values.iterator();
			int sum = 0;
			List<String> allUsers = new ArrayList<String>();
			String[] users;
			while(it.hasNext())
			{
				users = it.next().toString().split("#");
				sum += users.length;
				allUsers.addAll(Arrays.asList(users));
			}
			try {
				if(sum > 50 && !allUsers.isEmpty()){	
					context.write(key, new Text(StringUtils.join(allUsers,",")));
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

	public static void main(String[] args) throws Exception {
		run(args);
	}

	public static void run(String[] args) throws Exception {
		Configuration conf = new Configuration();
	
		// create a new Job
		Job job = new Job(conf, "Apriori");
		job.setJarByClass(SongCounter.class);
		
		// set the intermediate key and value types
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// set the mapper and the reducer
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		// one reducer only, this MR will create the partition file
		//job.setNumReduceTasks(1);
		// sets the input format for the job and the output format
//		job.setInputFormatClass(TextInputFormat.class);
//		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		SequenceFileOutputFormat.setOutputPath(job, new Path(args[1]));

		// Submit the job to the cluster and wait for it to finish.
		System.exit((job.waitForCompletion(true))?0:1);
	}
}