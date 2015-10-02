
import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigInteger;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.text.DateFormat;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.examples.SecondarySort.IntPair;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.opencsv.CSVParser;

public class Hpopulate{
	
	//the columm names array used to populate the entire fields present in a row
 static	String[] colNames = new String[]{"Year", "Quarter" , "Month", "DayofMonth" ,"DayOfWeek",
			"FlightDate", "UniqueCarrier","AirlineID","Carrier" , "TailNum" ,"FlightNum",
			"Origin" , "OriginCityName" , "OriginState" ,"OriginStateFips" ,"OriginStateName",
			"OriginWac", "Dest" , "DestCityName" , "DestState" , "DestStateFips" , "DestStateName",
			"DestWac" , "CRSDepTime" ,"DepTime" , "DepDelay" , "DepDelayMinutes" , "DepDel15",
			"DepartureDelayGroups" , "DepTimeBlk", "TaxiOut" , "WheelsOff" , "WheelsOn" ,
			"TaxiIn" , "CRSArrTime" , "ArrTime" , "ArrDelay" , "ArrDelayMinutes" , "ArrDel15",
			"ArrivalDelayGroups" , "ArrTimeBlk" , "Cancelled" , "CancellationCode", "Diverted",
			"CRSElapsedTime" , "ActualElapsedTime" , "AirTime" ,"Flights" , "Distance" , "DistanceGroup",
			"CarrierDelay" , "WeatherDelay" , "NASDelay" , "SecurityDelay" , "LateAircraftDelay"};
	
 //the mapper class used for HPopulate
 /* to minimize the overhead of setting up and maintaining the connections
  * I do the connections in the setup() method and close the connection in
  * cleanup() method.I have specified left padding in month.
  * 
  * 
  */
	public static class PopulateMapper extends
			Mapper<Object, Text, Text, Text> {

		CSVParser parser = new CSVParser();
		Configuration conf = HBaseConfiguration.create();
		//the Hbase table
		HTable table = null;
		
		@Override
		protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context)
				throws IOException, InterruptedException {
			super.setup(context);
			table = new HTable(conf, "AllFlights");
			table.setAutoFlush(false);
		}
		
		//map function
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			try {			
				// below is the code for parsing the input that we get as a value
				// for the map call
				String flightInfo[] = null;
				// Construct CSVParser using a comma for the separator
				CSVParser parser = new CSVParser();
				// parse the line and store the parsed values in flightInfo string
				// array
				flightInfo = parser.parseLine(value.toString());
				// yearString represents year 
				String year = flightInfo[0];
				// Unique carrier
				String unique = flightInfo[6];
				//the month specified with padding of 2
				String month = StringUtils.leftPad(flightInfo[2], 2, "0");
				//making the key by using year, unique carrier, month and key 
				//where key is the offset of the line given to map function as a input
				//where the key uniquely identifies a record
				byte[] Key = Bytes.toBytes(year + "," + unique + "," + month + "," + key);
				//the make the row key
				Put p = new Put(Key);
				
				//to populate all the fields that are in a record
				for (int i = 0; i < flightInfo.length; i++) {
					p.add(Bytes.toBytes("flightInfo"),
							Bytes.toBytes(colNames[i]),
							Bytes.toBytes(flightInfo[i]));
                   
				}
				//put each record in table
				table.put(p);
			}  catch (Exception e) {
				e.printStackTrace();
			}
		}

		@Override
		protected void cleanup(
				org.apache.hadoop.mapreduce.Mapper.Context context)
				throws IOException, InterruptedException {
			super.cleanup(context);
			table.close();
		}

		
	}

	
    //the main function
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 1) {
			System.err.println("Usage: Hpopulate <in> <out>");
			System.exit(2);
		}
		try {
			//the configurations required to setup up connection and create table
			HBaseConfiguration hc = new HBaseConfiguration(conf);
			HTableDescriptor ht = new HTableDescriptor("AllFlights");
			ht.addFamily(new HColumnDescriptor("flightInfo"));
			HBaseAdmin hba = new HBaseAdmin(hc);
			hba.createTable(ht);
		} catch (Exception e) {
			e.printStackTrace();
		}
		Job job = new Job(conf, "Hpopulate");
		job.setJarByClass(Hpopulate.class);
		job.setMapperClass(PopulateMapper.class);	
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
