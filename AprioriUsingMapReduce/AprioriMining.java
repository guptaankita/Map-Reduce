package com.trial;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

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
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class AprioriMining {

	public static class AprioriMapper extends
			Mapper<Object, Text, ItemsetKey, Text> {

		// map function
		@Override
		public void map(Object key, Text line, Context context)
				throws IOException, InterruptedException {
			String input = line.toString().trim();
			String[] itemsetUserSet = input.split("\t");
			String[] split = itemsetUserSet[0].split(",");
			int i;
			String[] prefix = new String[split.length - 1];
			for (i = 0; i < prefix.length; i++) {
				prefix[i] = split[i];
			}
			ItemsetKey outKey = new ItemsetKey();
			outKey.setPrefix(StringUtils.join(prefix, ","));
			outKey.setSong(itemsetUserSet[0]);
			context.write(outKey, new Text(itemsetUserSet[1]));
		}

	} // end of mapper

	public static class AprioriReducer extends
			Reducer<ItemsetKey, Text, Text, Text> {

		HashSet<String> itemsets;
		String oneLine;
		Double minSup;

		/*
		 * Loads all the itemsets of length k-1 for pruning in reduce function.
		 * 
		 * */
		@Override
		protected void setup(
				Reducer<ItemsetKey, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			minSup = Double.parseDouble(context.getConfiguration()
					.get("minSup"));
			itemsets = new HashSet<String>();
			FileSystem fs = FileSystem.getLocal(context.getConfiguration());
			Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context
					.getConfiguration());
			BufferedReader cacheReader = new BufferedReader(
					new InputStreamReader(fs.open(cacheFiles[0])));
			String line;
			while ((line = cacheReader.readLine()) != null) {
				oneLine = line.split("\t")[0];
				itemsets.add(oneLine);
			}
			cacheReader.close();
			fs.close();
			super.setup(context);
		}

		@Override
		protected void reduce(ItemsetKey key, Iterable<Text> values,
				Reducer<ItemsetKey, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			Iterator<Text> it = values.iterator();
			List<String> sets = new ArrayList<String>();
			List<String> listners = new ArrayList<String>();
			String val;
			while (it.hasNext()) {
				listners.add(it.next().toString());
				sets.add(key.getSong());
			}
			int i, j, z, count;
			String[] songs1, songs2, resultSong, listeners1, listeners2;
			Long[] listeners, commons;
			for (i = 0; i < sets.size() - 1; i++) {
				songs1 = sets.get(i).split(",");
				listeners1 = listners.get(i).split(",");
				for (j = i + 1; j < sets.size(); j++) {
					songs2 = sets.get(j).split(",");
					listeners2 = listners.get(j).split(",");
					if(listeners1.length >= listeners2.length)
					{
						listeners = new Long[listeners1.length];
						for(int p=0; p<listeners1.length; p++)
						{
							listeners[p] = Long.parseLong(listeners1[p]);
						}
						commons = findCommon(listeners2, listeners);
						if(commons.length < 1)
						{
							continue;
						}	
					}
					else 
					{
						listeners = new Long[listeners2.length];
						for(int p=0; p<listeners2.length; p++)
						{
							listeners[p] = Long.parseLong(listeners2[p]);
						}
						commons = findCommon(listeners1, listeners);
						if(commons.length < 1)
						{
							continue;
						}	
					}
					resultSong = new String[songs1.length + 1];
					for (z = 0; z < songs1.length; z++) {
						resultSong[z] = songs1[z];
					}
					resultSong[z] = songs2[songs2.length - 1];
					
					// get subsets for pruning
					List<String> subsets = getSubsets(resultSong);
					// Pruning lookup
					if(search(subsets))
					{
						String outKey = StringUtils.join(resultSong, ",");
						String outVal = StringUtils.join(commons, ",");
						context.write(new Text(outKey), new Text(outVal));
					}	
				}
			}
		}

		/*
		 * Search all subsets in the itemsets. This is brute force pruning
		 * */
		private boolean search(List<String> songs) {
			for (int i = 0; i < songs.size(); i++) {
				if(!itemsets.contains(songs.get(i)))
				{
					return false;
				}
			}
			return true;
		}
		
		private void getSubsets(List<String> superSet, int k, int idx, List<String> current,List<List<String>> solution) {
		    //successful stop clause
		    if (current.size() == k) {
		        solution.add(new ArrayList<>(current));
		        return;
		    }
		    //unseccessful stop clause
		    if (idx == superSet.size()) return;
		    String x = superSet.get(idx);
		    current.add(x);
		    //"guess" x is in the subset
		    getSubsets(superSet, k, idx+1, current, solution);
		    current.remove(x);
		    //"guess" x is not in the subset
		    getSubsets(superSet, k, idx+1, current, solution);
		}

		public List<List<String>> getSubsets(List<String> superSet, int k) {
		    List<List<String>> res = new ArrayList<>();
		    getSubsets(superSet, k, 0, new ArrayList<String>(), res);
		    return res;
		}
		
		public List<String> getSubsets(String[] itemset)
		{
			List<String> pruneSets = new ArrayList<String>();
			String[] subItemset = new String[itemset.length - 1];
			for(int i=0; i< subItemset.length; i++)
			{
				subItemset[i] = itemset[i];
			}
			List<List<String>> subset = getSubsets(Arrays.asList(subItemset), subItemset.length - 1);
			ArrayList<String> temp;
			for(List<String> item : subset)
			{
				Collections.sort(item);
				item.add(itemset[itemset.length - 1]);
				pruneSets.add(StringUtils.join(item,","));
			}
			return pruneSets;
		}
		
		/*
		 * Find the intersection between list of userIds of two different itemsets
		 * */
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

	}

	public static class AprioriPartitioner extends
			Partitioner<ItemsetKey, Text> {

		@Override
		public int getPartition(ItemsetKey key, Text value, int numPartitions) {
			// TODO Auto-generated method stub
			return Math.abs(key.hashCode()) % numPartitions;
		}
	}

	public static class KeyComparator extends WritableComparator {
		protected KeyComparator() {
			super(ItemsetKey.class, true);
		}

		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			ItemsetKey ip1 = (ItemsetKey) w1;
			ItemsetKey ip2 = (ItemsetKey) w2;
			int cmp = ip1.getPrefix().compareToIgnoreCase(ip2.getPrefix());
			if (cmp != 0) {
				return cmp;
			}
			return ip1.getSong().compareTo(ip2.getSong());
		}
	}

	// this will group the values from mapper based on the carrier
	public static class GroupComparator extends WritableComparator {
		protected GroupComparator() {
			super(ItemsetKey.class, true);
		}

		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			ItemsetKey ip1 = (ItemsetKey) w1;
			ItemsetKey ip2 = (ItemsetKey) w2;
			return ip1.getPrefix().compareToIgnoreCase(ip2.getPrefix());
		}
	}

	public static void main(String[] args) throws Exception {

		double minSupport = 0;
		String[] tempArr;
		List<String[]> prevItems = new ArrayList<String[]>();

		for (int pass = 3; pass <= 7; pass++) {

			String input = args[0] + (pass - 1);
			String output = args[0] + pass;

			boolean isDone = runJob(input, output, pass, minSupport);
			if (!isDone) {
				System.err.println("MapReduce job failed. Exiting !!");
				System.exit(1);
			}
		}
		System.exit(0);
	}

	public static boolean runJob(String inputDir, String outputDir, int pass,
			double minSupport) throws Exception {
		boolean jobStatus = false;

		Configuration conf = new Configuration();
		conf.setInt("passNum", pass);
		conf.set("minSup", Double.toString(minSupport));
		System.out.println("Starting AprioriPhase " + pass + " Job");
		Job job = new Job(conf, "AprioriPhase" + pass);
		job.setJarByClass(AprioriMining.class);
		job.setMapperClass(AprioriMapper.class);
		job.setReducerClass(AprioriReducer.class);
		job.setMapOutputKeyClass(ItemsetKey.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setPartitionerClass(AprioriPartitioner.class);
		job.setSortComparatorClass(KeyComparator.class);
		job.setGroupingComparatorClass(GroupComparator.class);

		FileInputFormat.addInputPath(job, new Path(inputDir));
		SequenceFileOutputFormat.setOutputPath(job, new Path(outputDir));
		URI uri = URI.create(inputDir);
		FileSystem fs = FileSystem.get(uri, job.getConfiguration());   
		

		//Add single songs file
		FileStatus[] fileStatus = fs.listStatus(new Path(inputDir));
		// 4. Using FileUtil, getting the Paths for all the FileStatus
		Path[] paths = FileUtil.stat2Paths(fileStatus);
		// 5. Iterate through the directory and display the files in it
		int songsFileCount = 0;
		for (Path path : paths) {
			DistributedCache.addCacheFile(path.toUri(), job.getConfiguration());
		}

		jobStatus = (job.waitForCompletion(true) ? true : false);
		System.out.println("Finished AprioriPhase " + pass + " Job");

		return jobStatus;
	}
}

// The class to represent the composite key
class ItemsetKey implements WritableComparable<ItemsetKey> {
	private String prefix;
	private String song;

	public String getPrefix() {
		return prefix;
	}

	public void setPrefix(String prefix) {
		this.prefix = prefix;
	}

	public String getSong() {
		return song;
	}

	public void setSong(String song) {
		this.song = song;
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		this.prefix = arg0.readUTF();
		this.song = arg0.readUTF();
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		arg0.writeUTF(prefix);
		arg0.writeUTF(song);
	}

	@Override
	public int compareTo(ItemsetKey o) {
		int res = this.prefix.compareToIgnoreCase(o.getPrefix());
		if (res == 0) {
			res = this.song.compareTo(o.getSong());
		}
		return res;
	}

	public int hashCode() {
		return this.prefix.hashCode();
	}

	public boolean equals(Object o) {
		ItemsetKey p = (ItemsetKey) o;
		return this.prefix.equals(p.getPrefix());
	}

	public Text toText() {
		return new Text("(" + this.prefix.toString() + ","
				+ this.song.toString() + ")");
	}

}