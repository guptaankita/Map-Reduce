import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;



public class NoCombiner {

	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				// store the value of itr.nextToken in a string "test"
				String test = itr.nextToken();
				// store in "first" the first character of the String
				char first = test.charAt(0);
				// convert the first character to Lowercase
				first = Character.toLowerCase(first);
				// check if the word is a real word i,e it starts with
				// m,n,o,p,q(case insensitive as we
				// change the first character to lowercase above. If the words
				// starts with m,n,o,p,q
				// then write to the local disk for the worker.
				if (first == 'm' || first == 'n' || first == 'p'
						|| first == 'o' || first == 'q') {
					word.set(test);
					context.write(word, one);
				}
			}
		}
	}

	// class customPartitioner extends Partitioner class and overrides the
	// "getPartition() method
	// getPartition() method takes in three argument key, value and reducetasks
	// key and value are the intermediate key and value produced by the map
	// function
	// reducetasks is the number of reducers used in the mapreduce program
	// we assign the key to the partition 0,1,2 based on the first character of
	// the key
	// If the first character start with 'm' we assign it to partition 1,if the
	// first character starts with
	// 'n' we assign it to partition 2 and so on
	// if the number of reducers are 0 then return 0 to avoid divide by zero
	// exception
	// we have done partition number modulo reducetasks to avoid illegal
	// partitions when the
	// system has lesser reducers than the assigned number.
	public static class customPartitioner extends
			Partitioner<Text, IntWritable> {

		@Override
		public int getPartition(Text key, IntWritable value, int reducetasks) {
			if (reducetasks == 0)
				return 0;
			if (key.charAt(0) == 'm' || key.charAt(0) == 'M')
				return 0;
			else if (key.charAt(0) == 'n' || key.charAt(0) == 'N')
				return 1 % reducetasks;
			else if (key.charAt(0) == 'o' || key.charAt(0) == 'O')
				return 2 % reducetasks;
			else if (key.charAt(0) == 'p' || key.charAt(0) == 'P')
				return 3 % reducetasks;
			else if (key.charAt(0) == 'q' || key.charAt(0) == 'Q')
				return 4 % reducetasks;
			else
				return 0;
		}
	}

	public static class IntSumReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "word count");
		job.setJarByClass(NoCombiner.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setPartitionerClass(customPartitioner.class);
		// set the number of reducers to 5
		job.setNumReduceTasks(5);
		// Disable the Combiner
		// job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
