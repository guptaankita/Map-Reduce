import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import com.opencsv.CSVParser;

public class Plain {

	public static class JoinMapper extends Mapper<Object, Text, Text, Text> {
		private Text outkey = new Text();
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
			// FlightDate is at column 6
			String FlightDate = flightInfo[5];
			// origin is at column 12
			String origin = flightInfo[11];
			// dest is destination at column 18
			String dest = flightInfo[17];
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
			// diverted is at column 44
			String diverted = flightInfo[43];
			// deptime represents deparature time which is at column 25
			String deptime = flightInfo[24];
			// arrtime represents arrival time which is at column 36
			String arrtime = flightInfo[35];
			// ArrDelayMinutes represents arrival time which is at column 38
			String ArrDelayMinutes = flightInfo[37];

			// to check whether the string is empty or not
			// if it is empty we do not proceed
			if (arrtime.length() > 0 && deptime.length() > 0
					&& origin.length() > 0 && ArrDelayMinutes.length() > 0
					&& dest.length() > 0)

			{
				/*
				 * Check whether the origin is "ORD"Check whether if the flight
				 * date is between june 2007 and may 2008 both included Check
				 * whether the flight is cancelled and diverted Emit the
				 * arrtime(arrival time),ArrDelayMinutes and Destination for the
				 * records that satisfies the above mentioned steps "1"
				 * represents the tag that it belongs to the first list the key
				 * for the emitted record is flight date
				 */
				if ((origin.equals("ORD"))
						&& ((year == 2008 && month <= 5) || (year == 2007 && month >= 6))) {
					if ((!cancelled.equals("1")) && (!diverted.equals("1"))) {
						outkey.set(FlightDate);
						outvalue.set("1" + arrtime + "," + ArrDelayMinutes
								+ "," + dest);
						context.write(outkey, outvalue);

					}
				}

				/*
				 * Check whether the destination is "JFK"Check whether if the
				 * flight date is between june 2007 and may 2008 both included
				 * Check whether the flight is cancelled and diverted Emit the
				 * deptime(departure time),ArrDelayMinutes and origin for the
				 * records that satisfies the above mentioned steps "2"
				 * represents the tag that it belongs to the second list the key
				 * for the emitted record is flight date
				 */
				else if (dest.equals("JFK")
						&& ((year == 2008 && month <= 5) || (year == 2007 && month >= 6)))

				{
					if ((!cancelled.equals("1")) && (!diverted.equals("1")))

					{
						outkey.set(FlightDate);
						outvalue.set("2" + deptime + "," + ArrDelayMinutes
								+ "," + origin);
						context.write(outkey, outvalue);
					}
				}
			}
		}

	}

	// to implement reducer

	public static class JoinReducer extends Reducer<Text, Text, Text, Text> {
		private ArrayList<Text> listS1 = new ArrayList<Text>();
		private ArrayList<Text> listS2 = new ArrayList<Text>();

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// Clear our lists
			listS1.clear();
			listS2.clear();
			// iterate through all the values , grouping all records based on
			// what it was tagged with
			for (Text t : values) {
				if (t.charAt(0) == '1')
					listS1.add(new Text(t.toString().substring(1)));
				else if (t.charAt(0) == '2')
					listS2.add(new Text(t.toString().substring(1)));

			}// end of foreach

			/*
			 * Iterate over the entire listS1 and listS2 to find records where
			 * the ArrTime(arrival time) of flight in listS1 is less than the
			 * DepTime(departure time) of flight in listS2 And destination
			 * airport(dest) of flight in listS1 is equal to the origin
			 * airport(origin) of flight in listS2 if the above conditions meet
			 * add up the ArrDelayMinutes of flights in each list that satisfies
			 * the above mentioned steps
			 */
			for (Text A : listS1) {
				// as I am passing a comma separated value from map
				// I split it here on "," to get each values
				String Avalue[] = A.toString().split(",");
				String arrTime = Avalue[0];
				String Adelay = Avalue[1];
				int ArrTime = Integer.parseInt(arrTime);
				String dest = Avalue[2];

				for (Text B : listS2) {
					// as I am passing a comma separated value from map
					// I split it here on "," to get each values
					String Bvalue[] = B.toString().split(",");
					String depTime = Bvalue[0];
					String Bdelay = Bvalue[1];
					String origin = Bvalue[2];
					int DepTime = Integer.parseInt(depTime);
					if (ArrTime < DepTime) {
						if (dest.equals(origin)) {
							/*
							 * add up the arrDelayMinutes of each pair of
							 * flights which satisfies the above condition
							 */
							double total = Double.parseDouble(Adelay)
									+ Double.parseDouble(Bdelay);
							Text value = new Text(Double.toString(total));
							context.write(key, value);

						}
					}

				}
			}
		}

	}

	// main function
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: Plain <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "Plain");
		job.setJarByClass(Plain.class);
		job.setMapperClass(JoinMapper.class);
		job.setReducerClass(JoinReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
