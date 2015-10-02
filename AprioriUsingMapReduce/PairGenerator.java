package com.trial;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
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

public class PairGenerator {

	public static class Map extends Mapper<Object, Text, Text, Text> {

		String[] tokens = null;
		TreeMap<String,Long[]> songs;
		String line;
		String[] split;
		String[] users;
		Long[] usersLong;
		

		@Override
		protected void setup(Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			try {				
				songs = new TreeMap<String, Long[]>();

				FileSystem fs = FileSystem.getLocal(context.getConfiguration());
				Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context
						.getConfiguration());
				BufferedReader cacheReader;
				for(int i = 0; i< cacheFiles.length; i++)
				{
					cacheReader = new BufferedReader(
							new InputStreamReader(fs.open(cacheFiles[i])));
					
					while ((line = cacheReader.readLine()) != null) {
						split = line.split("\t");
						users = split[1].split(",");
						usersLong = new Long[users.length];
						for(int j=0; j< users.length; j++)
						{
							try {
								usersLong[j] = Long.parseLong(users[j]);
						    } catch (NumberFormatException nfe) {};
						}
						songs.put(split[0], usersLong);
					}
					cacheReader.close();
				}
				fs.close();
			} catch (IOException ioe) {
				System.out
						.println("IOException reading from distributed cache");
				System.out.println(ioe.toString());
			}

		} // end of setup()

		// map function
		@Override
		public void map(Object key, Text line, Context context)
				throws IOException, InterruptedException {
			boolean found = false;
			String[] pair;
			Long[] commons;
			Set<Entry<String, Long[]>> songEntry = songs.entrySet();
			for(Entry<String, Long[]> entry : songEntry)
			{
				if(found)
				{
					split = line.toString().split("\t");
					users = split[1].split(",");
					commons = findCommon(users, entry.getValue());
					if(commons.length > 10)
					{
						pair = new String[2];
						pair[0] = split[0];
						pair[1] = entry.getKey();
						String outKey = StringUtils.join(pair, ",");
						context.write(new Text(outKey), new Text(StringUtils.join(commons, ",")));
					}
					
				}
				else if(line.toString().split("\t")[0].equals(entry.getKey())){
					found = true;
				}
			}
		}
		
		private Long[] findCommon(String[] arrayOne, Long[] arrayTwo)
		{
			Long[ ] arrayToHash;
	    		arrayToHash = arrayTwo;
	    	
	    	long converted;
	        HashSet<Long> intersection = new HashSet<Long>( );
 
	        HashSet<Long> hashedArray = new HashSet<Long>( );
	        for( Long entry : arrayToHash ) {
	            hashedArray.add( entry );
	        }
 
	        for( String entry : arrayOne ) {
	        	converted = Long.parseLong(entry);
	            if( hashedArray.contains( converted ) ) {
	                intersection.add( converted );
	            }
	        }
 
	        return intersection.toArray( new Long[ 0 ] );
		}

	} // end of mapper

	public static void main(String[] args) throws Exception {
		run(args);
	}

	public static void run(String[] args) throws Exception {
		Configuration conf = new Configuration();

		// create a new Job
		Job job = new Job(conf, "Apriori");
		job.setJarByClass(PairGenerator.class);
		
		
		// 2. Get the instance of the HDFS
		URI uri = URI.create(args[0]);
		FileSystem fs = FileSystem.get(uri, job.getConfiguration());   
		

		//Add single songs file
		FileStatus[] fileStatus = fs.listStatus(new Path(args[0]));
		// 4. Using FileUtil, getting the Paths for all the FileStatus
		Path[] paths = FileUtil.stat2Paths(fileStatus);
		// 5. Iterate through the directory and display the files in it
		int songsFileCount = 0;
		for (Path path : paths) {
			DistributedCache.addCacheFile(path.toUri(), job.getConfiguration());
		}
//		job.getConfiguration().setInt("songsFileCount", songsFileCount);
//		//add transactions file
//		uri = URI.create(args[1]);
//		fs = FileSystem.get(uri, job.getConfiguration());   
//		fileStatus = fs.listStatus(new Path(args[1]));
//		// 4. Using FileUtil, getting the Paths for all the FileStatus
//		paths = FileUtil.stat2Paths(fileStatus);
//		// 5. Iterate through the directory and display the files in it
//		for (Path path : paths) {
//			DistributedCache.addCacheFile(path.toUri(), job.getConfiguration());
//		}
		// set the intermediate key and value types
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// set the mapper and the reducer
		job.setMapperClass(Map.class);
		//job.setReducerClass(Reduce.class);

		// one reducer only, this MR will create the partition file
		// job.setNumReduceTasks(1);
		// sets the input format for the job and the output format
		// job.setInputFormatClass(TextInputFormat.class);
		// job.setOutputFormatClass(SequenceFileOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		SequenceFileOutputFormat.setOutputPath(job, new Path(args[1]));

		// Submit the job to the cluster and wait for it to finish.
		System.exit((job.waitForCompletion(true)) ? 0 : 1);
	}
}