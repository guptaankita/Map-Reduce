
public class Hcompute {
	//the mapper class
	public static class HcomputeMapper 
	extends TableMapper<Text, Text>{

		
		private Text outkey = new Text();
		private Text outvalue = new Text();


		public void map(ImmutableBytesWritable key, Result value, Context context
				) throws IOException, InterruptedException {

			//ImmutableBytesWritable sets the value of key to a given byte range
    ImmutableBytesWritable userKey = new ImmutableBytesWritable(key.get(), 0, Bytes.SIZEOF_INT);
			
           //convert the userKey to string
			String Key = Bytes.toString(userKey.get());
       
		   //split the Key as key is composed of year,unique carrier, month and offset	
		    String[] KeyDetails = Key.split(",");
		    
		    //emit the key as a combination of unique carrier and month
		    outkey.set(KeyDetails[1]+ ","+ KeyDetails[2]);
		    //emit the value as ArrDelayMinutes
		    outvalue.set(value.getValue("flightInfo".getBytes(), "ArrDelayMinutes".getBytes()));
		
				context.write(outkey, outvalue);
			
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
		public static class HcomputeReducer extends Reducer<Text, Text, Text, Text> {

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
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 1) {
			System.err.println("Usage: Hcompute <out>");
			System.exit(2);
		}

		//Set scan object
		  // don't set to true for MR jobs
		
		//adding a filter to reduce the amount of data transfered
		FilterList filterTable = new FilterList(FilterList.Operator.MUST_PASS_ALL);
		//to filter the records where the ArrDelayMinutes is empty
		SingleColumnValueFilter DelayMinutesFilter = new SingleColumnValueFilter("flightInfo".getBytes(),"ArrDelayMinutes".getBytes(),CompareOp.NOT_EQUAL,"".getBytes());
		//to filter the records which are cancelled
		SingleColumnValueFilter CancelledFilter = new SingleColumnValueFilter("flightInfo".getBytes(),"Cancelled".getBytes(),CompareOp.EQUAL,"0.00".getBytes());
		//add both the filters to filer list
		filterTable.addFilter(DelayMinutesFilter);
		filterTable.addFilter(CancelledFilter);

		
		
		//scan the row where the year is 2008
		Scan scan = new Scan(Bytes.toBytes("2008,"));
		scan.setCaching(500);    
		scan.setCacheBlocks(false); 
		scan.setFilter(filterTable);
		
		Job job = new Job(conf, "Hcompute");
		job.setJarByClass(Hcompute.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapperClass(HcomputeMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setPartitionerClass(custompartitioner.class);
		job.setReducerClass(HcomputeReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		TableMapReduceUtil.initTableMapperJob(
				"AllFlights",      
				scan,
				HcomputeMapper.class,   
				Text.class,
				Text.class,
				job);
		
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[0]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}
	
	

	
	
	

}
