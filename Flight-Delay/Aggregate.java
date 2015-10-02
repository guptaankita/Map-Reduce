import java.io.IOException;
import java.util.Iterator;
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

//Map program to calculate the aggregate of flight delays that are outputted from the plain program
//Mapper
public class Aggregate {
	public static class AggregateMapper extends
			Mapper<Object, Text, Text, Text> {
		private Text record = new Text();
		// make a "dummy" key so that all the record goes to the same reducer
		private Text dummy = new Text("dummy");

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			record.set(value);
      //emit all the records without any change
			context.write(dummy, record);
		}
	}
   //Reducer
	public static class AggregateReducer extends Reducer<Text, Text, Text, Text> {
		//count to hold the total number of pairs
		int count = 0;
		private Text result = new Text();
		//sum to hold the aggregate values of all the delays
		double sum = 0;

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
            //iterate over all the values
			for (Text val : values) {
				/*As the output file of Plain program contains two column
				 * the first column contains the the flight date and the
				 * second column contains the delay , as we need just the
				 * delay , I store the value in a array, and store the last 
				 * element of array as a string.
				 * Convert the string to a double value and sum it
				 * Increment the count
				 */
				String delay = val.toString();
				String matches[] = delay.split("\\t");
				String lastitem = matches[matches.length - 1];
				double delaytime = Double.parseDouble(lastitem);
				sum = sum + delaytime;
				++count;
			}
			//compute the average
			double avg = sum / count;
			result.set(Double.toString(avg));
			key.set(Integer.toString(count));
			//emit the average that is total flight delay
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: Aggregate <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "Aggregate");
		job.setJarByClass(Aggregate.class);
		job.setMapperClass(AggregateMapper.class);
		job.setReducerClass(AggregateReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
