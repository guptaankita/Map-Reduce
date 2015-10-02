import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Secondary {
	
	//the mapper class
	public static class SecondaryMapper extends Mapper<Object, Text, Text, Text> {
		//the key to be used
		private Text outkey = new Text();
		//the value to be send with the key
		private Text outvalue = new Text();

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// below is the code for parsing the input that we get as a value
			// for the map call
			String flightInfo[] = null;
			// Construct CSVParser using a comma for the separator
			CSVParser parser = new CSVParser();
			// parse the line and store the parsed values in flightInfo string
			// array
			flightInfo = parser.parseLine(value.toString());
		// monthString represent month which is at column 3
			String monthString = flightInfo[2];
			// yearString represents year which is at column 
			String yearString = flightInfo[0];
			// parsing yearString as integer
			int year = Integer.parseInt(yearString);
			// parsing monthString as integer
			int month = Integer.parseInt(monthString);
			// cancelled is at column 42
			String cancelled = flightInfo[41];
			// ArrDelayMinutes represents arrival time which is at column 38
			String ArrDelayMinutes = flightInfo[37];
			//the unique carrier information for each flight
			String unique = flightInfo[6];
			//filter, ArrDelayMinutes should not be empty string, year should be 2008 and the
			//flight should not be cancelled
			if (ArrDelayMinutes.length()>0 && (year == 2008) && (cancelled.equals("0.00"))) {
				//the key is a combination of unique carrier and month
				outkey.set(unique + "," + monthString);
				outvalue.set(ArrDelayMinutes);
				context.write(outkey,outvalue);
			}
			}
	}
	
	//partitioner
    /* the custom partitioner designed to partition based on the unique carrier of 
     * the flight.As the key is a combination of unique carrier and month separated
     * by commas, I split it on comma, get the first part which is unique carrier
     * and apply hashCode() on the unique carrier modules num, where num is the
     * number of reducer.
     */
	public static class custompartitioner extends Partitioner<Text, Text> {
		@Override
		public int getPartition(Text key, Text value, int num) {
			String[] keyDetails = key.toString().split(",");
			String unique = keyDetails[0];
			return (unique.hashCode() % num);
		}
	}
	
	//key comparator
	/* it is used to sort the data based on both the unique carrier and month
	 * it sorts data first by unique carrier and then it sorts it by month
	 * so for each unique carrier it then sorts by month in increasing order
	 */
	public static class KeyComparator extends WritableComparator {
		protected KeyComparator() {
			super(Text.class, true);
		}

		public int compare(WritableComparable a, WritableComparable b) {
			Text key1 = (Text) a;
			Text key2 = (Text) b;
			String[] forKey1 = key1.toString().split(",");
			String[] forKey2 =  key2.toString().split(",");
			int comp = forKey1[0].compareTo(forKey2[0]);
			if (comp == 0) {
				comp = forKey1[1].compareTo(forKey2[1])
			}
			return comp;
		}
	}
	
	//grouping comparator
	/* Grouping comparator groups the keys based on unique carrier
	 * it groups the records based on unique carrier
	 */
	public static class GroupComparator extends WritableComparator {
		protected GroupComparator() {
			super(Text.class, true);
		}

		public int compare(WritableComparable a, WritableComparable b) {
			Text key1 = (Text) a;
			Text key2 = (Text) b;
			String[] forkey1 = key1.toString().split(",");
			String[] forkey2 = key2.toString().split(",");
			return (forkey1[0].compareTo(forkey2[0]));
		}
	}

	//Reducer 
	/* Each reducer get the records for a unique carrier,sorted by month
	 * the reducer is used to calculate delays for each unique carrier
	 * I included two hash maps one for calculating the delay for each month
	 * for a unique carrier and one for maintaining the count for each month
	 * then I iterate over the values to get the corresponding key, I extract from
	 * the corresponding key the month.
	 * then I iterate over the hash map to sum over the delays and then calculate
	 * the average delay. I use the Math.ceil() function to round up the values.
	 * I used String allDelays to delay the delay in the format specified in the
	 * assignment. 
	 */
	public static class SecondaryReducer extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			
			
			String allDelays = "";
			HashMap<Integer, Double> delaySum = new HashMap<Integer, Double>();
			HashMap<Integer, Integer> count = new HashMap<Integer, Integer>();
			String[] keyDetails = key.toString().split(",");
			for (Text val : values) {
				String [] keyVal = key.toString().split(",");
				Integer month = Integer.parseInt(keyVal[1]);
				Double delay = Double.parseDouble(val.toString());
			
				if (!delaySum.containsKey(month)) {
					delaySum.put(month, delay);
					count.put(month, 1);
				} else {
					delaySum.put(month, (delaySum.get(month) + delay));
					count.put(month, count.get(month) + 1);
				}
			}
			for (Integer month : delaySum.keySet()) {
			double	avg = delaySum.get(month) / count.get(month);
				Integer averageDelay = (int) Math.ceil(avg);
				allDelays = allDelays + "(" + month.toString() + ","
						+ averageDelay.toString() + "),";
			}
			context.write(new Text(keyDetails[0]),
					new Text(allDelays));
		}
	}
		//main function
		public static void main(String[] args) throws Exception {

			Configuration conf = new Configuration();
			String[] otherArgs = new GenericOptionsParser(conf, args)
					.getRemainingArgs();
			if (otherArgs.length != 2) {

				System.err.println("Usage: Secondary <in> <out>");
				System.exit(2);
			}
			Job job = new Job(conf, "Secondary");
			job.setJarByClass(Secondary.class);
			job.setMapperClass(SecondaryMapper.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setPartitionerClass(custompartitioner.class);
			job.setSortComparatorClass(KeyComparator.class);
			job.setGroupingComparatorClass(GroupingComparator.class);
			job.setReducerClass(SecondaryReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
			FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
			job.waitForCompletion(true);

		}

	




}
