package comments;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.examples.WordCount;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.eclipse.core.internal.utils.Cache.Entry;

import au.com.bytecode.opencsv.CSVParser;

public class Kmeans {
		
	//global counter to keep a count during random sampling
	    static enum MoreIterations {
	            numberOfIterations
	        }

                //map class for Kmeans
				public static class MapKmeans extends
				Mapper<Object, Text, DataPointKey, DataPoint> {

                      /*ArrayList of centers to hold the initial centroids 
                       * from the file in distributed cache                 
                       */
						static ArrayList<DataPointKey> centers = new ArrayList<DataPointKey>();
						//csvparser to parse the comma separted value
					    private CSVParser csvParser = new CSVParser(',','"');

						@Override
						protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context)
								throws IOException, InterruptedException {
							// TODO Auto-generated method stub
							super.setup(context);
							

							//Path array which stores the path of the files which contains the centroids
							Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
							ArrayList<Path> paths = new ArrayList<Path>();
							for (Path cacheFile : cacheFiles) {
							paths.add(cacheFile);
							}
							//clear the centers to hold the new centers
							centers.clear();	

						try {
							// Get centroid from Distributed cache
							for (Path p : paths) { 
									BufferedReader br = new BufferedReader(new FileReader(p.toString()));
									String line;
								try {
									while ((line = br.readLine()) != null) {
									String[] columns = line.split("\t");
									DataPointKey DataPoint = new DataPointKey(columns[0]);
										centers.add(DataPoint);									
								}
								}finally {
									br.close();
								}
				                       
							
								
										}
						} catch (IOException e) {
							System.err.println("IO Exception - Distributed cache: " + e.getMessage());
						}
					} //end of setup()

						  @Override
							public void map(Object key, Text line,Context context) throws IOException, InterruptedException {
								
								// Parse the input line which is input to map
								String[] parsedData = null;
								try {
									parsedData = this.csvParser.parseLine(line.toString());
								} catch (Exception e) {
									// In case of bad data record ignore them
									return;
								}
								
								DataPoint p1 = new DataPoint(parsedData[0],parsedData[1],parsedData[7],parsedData[8]);
								double min1 = Double.MAX_VALUE;
								double min2 = Double.MAX_VALUE;						 
								DataPointKey nearestCentroid= centers.get(0);
								for(DataPointKey p: centers){
									min1 = p1.Distance(p);				
									if (Math.abs(min1)< Math.abs(min2)) {
										nearestCenter = p;
										min2 = min1;
									}
								}
								context.write(nearestCentroid,p1);
							}

						}	//end of mapper
							
				//start of reducer
				public static class Reduce extends 
				Reducer<DataPointKey, DataPoint, DataPointKey, Text> {
					@Override
						protected void reduce(DataPointKey key, Iterable<DataPoint> values,
								Reducer<DataPointKey, DataPoint, DataPointKey, Text>.Context context)
								throws IOException, InterruptedException {
							
				
				double avgHotness = 0.0;
				double avgLoudness = 0.0;
				double avgBeat = 0.0;
				String DataPointInCluster = "";
				int total = 0;
				Text songId = null;
				Text clusterno = key.getClusterNumber();
				for(DataPoint value:values){
					songId = value.getSongID();
					avgHotness = avgHotness + value.getArtistHotnessinDouble();
					avgLoudness = avgLoudness + value.getLoudnessinDouble();
					avgBeat =avgBeat+value.getBeatinDouble();
					++total;
				}
				String t = Integer.toString(total);
				Double avg1Hotness = (avgHotness/total);
				Double avg1Loudness = (avgLoudness/ total);
				Double avg1Beat = (avgBeat/total);
				String Hotnessavg = Double.toString(avg1Hotness);
				String BeatAvg = Double.toString(avg1Beat);
				String LoudAvg = Double.toString(avg1Loudness);
				DataPointKey newCentroid = new DataPointKey(Hotnessavg,LoudAvg,BeatAvg);
				newCentroid.setClusterNumber(clusterno);
			   
				// Emit new center and DataPoints
				context.write(newCentroid, new Text(t));
			}
			}
				
				//Mapper class for random sampling
				//taken from the class module 5
				public static class SRSMapper extends
				Mapper<Object, Text, NullWritable, DataPointKey> {

			private Random rands = new Random();
			private Double percentage;

			@Override
			protected void setup(Context context) throws IOException,
					InterruptedException {
				// retrieve the percentage that is passed in via the configuration
				// like this: conf.set("filter_percentage", .5); for .5%
				
				percentage = 0.5/ 100.0;
			}

			@Override
			public void map(Object key, Text value, Context context)
					throws IOException, InterruptedException {

				if(rands.nextDouble() < percentage) {
					/*
					 * the "parameter" that we set equal to the value of k
					 */
				     String  myParam = context.getConfiguration().get("parameter");
	                     int p = Integer.parseInt(myParam);
	                   int check= (int) context.getCounter(MoreIterations.numberOfIterations).getValue();
	                   /*Check if the value of parameter is less than the value of K or not
	                    * if the parameter value is less than k increment the counter
	                    */
	                     if(check<p)
		                {context.getCounter(MoreIterations.numberOfIterations).increment(1);
		                   int number= (int) context.getCounter(MoreIterations.numberOfIterations).getValue();
	                     String[] columns = value.toString().split(",");
	                     //make the centroids
	                     DataPointKey centroids = new DataPointKey();
	                     centroids.setClusterNumber(new Text(String.valueOf(number)));
	                     centroids.setArtistHotness(new Text(columns[1]));
	                     centroids.setLoudness(new Text(columns[7]));
	                     centroids.setBeat(new Text(columns[8]));
	                     
					      context.write(NullWritable.get(), centroids);
		                }
				}
			}
		}

					public static void main(String[] args) throws Exception {

					String out = args[2];
					//vary k between 4 to 7
					for(int k=4;k<=7;k++){
						//run for 3 different initial configuartion
						for(int in = 0;in<3;in++){
						Configuration conf = new Configuration();
						String[] otherArgs = new GenericOptionsParser(conf, args)
								.getRemainingArgs();
						if (otherArgs.length != 3) {
							System.err.println("Usage: SRS <percentage> <in> <out>");
							System.exit(2);
						}
						conf.set("parameter", String.valueOf(k));
						Job job = new Job(conf, "SRS");
					    job.setJarByClass(Kmeans.class);
						job.setMapperClass(SRSMapper.class);
						job.setOutputKeyClass(NullWritable.class);
						job.setOutputValueClass(DataPointKey.class);
						job.setNumReduceTasks(0); // Set number of reducers to zero
						FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
						//name the output directory, include the k value and initial config
						//in the name of the directory
						otherArgs[1] = otherArgs[1] + String.valueOf(k)+ String.valueOf(in);
						FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
						job.waitForCompletion(true);
						otherArgs[2] = otherArgs[2] + String.valueOf(k)+ String.valueOf(in);
						boolean isDone = runJob(otherArgs);
					}
					}
					System.exit(1);
				}

				public static boolean runJob(String[] args) throws Exception {
		                         boolean jobStatus = false;
						Job job = new Job(new Configuration());
						Configuration conf = job.getConfiguration();
						
						String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
						if (otherArgs.length != 3) {
							System.err.println("Usage: kmeans <in> <centers> <out>");
							System.exit(2);
						}
                        //read the initial part m file generated from the random sampling
						Path pth = new Path(otherArgs[1]+ "/part-m-00000");
						//put the initial centroids in the distributed cache
						DistributedCache.addCacheFile(pth.toUri(), job.getConfiguration());

						job.setJarByClass(Kmeans.class);
						job.setMapperClass(MapKmeans.class);
						job.setReducerClass(Reduce.class);
						job.setMapOutputKeyClass(DataPointKey.class);
						job.setMapOutputValueClass(DataPoint.class);
						job.setOutputKeyClass(DataPointKey.class);
						job.setOutputValueClass(Text.class);
						int ConvergenceIteration = 1;
						String OutputIteration = "centroidOutputFolder" + ConvergenceIteration;

						FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
						FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

						boolean IsConverged = false;
						job.waitForCompletion(true);

						while(!IsConverged){

                                                      
							job = new Job(new Configuration());
							conf = job.getConfiguration();
							//create URI using the output folder
							URI uri = URI.create(otherArgs[2]);
							FileSystem fs = FileSystem.get(uri, job.getConfiguration());   
							//list to hold the centroid in the this iteration
		                  ArrayList<DataPointKey> prev = new ArrayList<DataPointKey>();
							FileStatus[] fileStatus = fs.listStatus(new Path(otherArgs[2]));
							/*
							 * Read contents from all part r files and add the centroids the 
							 * the previous list of centroids created above
							 */
							for(int i=0;i<fileStatus.length;i++)
							{  String p = fileStatus[i].getPath().toString();
							if (!p.contains("_SUCCESS")) {
								BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(fileStatus[i].getPath())));
								String line;
								try {
									while ((line = br.readLine()) != null) {
									String[] col = line.split("\t");
			          				String[] columns =col[0].split(",");
			          				DataPointKey dataPoint = new DataPointKey(columns[0],columns[1],columns[2],columns[3]);
									prev.add(dataPoint);
									
								}
								}finally {
									br.close();
								}
							}
							}
							//Using FileUtil, getting the Paths for all the FileStatus
							Path[] paths = FileUtil.stat2Paths(fileStatus);
							// Iterate through the directory and add all part-r files to the distributed cache
							for (Path path : paths) {
								DistributedCache.addCacheFile(path.toUri(), job.getConfiguration());
								
							}
							++ConvergenceIteration;
							job.setJarByClass(Kmeans.class);
							job.setMapperClass(MapKmeans.class);
							job.setReducerClass(Reduce.class);
							job.setMapOutputKeyClass(DataPointKey.class);
							job.setMapOutputValueClass(DataPoint.class);
							job.setOutputKeyClass(DataPointKey.class);
							job.setOutputValueClass(Text.class);
							FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
							otherArgs[2] = otherArgs[2] + ConvergenceIteration;
							FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
							job.waitForCompletion(true);
							//for reading the new centroids in the output file for this iteration
		         			URI uri1 = URI.create(otherArgs[2]);
							FileSystem fs1 = FileSystem.get(uri1, job.getConfiguration());   
							//arraylist to hold the new centroids created in this iteration
		                  ArrayList<DataPointKey> next = new ArrayList<DataPointKey>();
			                /*
			                 * read each part r files and put the centroids the "next" arraylist
			                 */
							FileStatus[] fileStatus1 = fs1.listStatus(new Path(otherArgs[2]));
							for(int i=0;i<fileStatus1.length;i++)
							{  String p = fileStatus1[i].getPath().toString();
							if (!p.contains("_SUCCESS")) {
								BufferedReader br = new BufferedReader(new InputStreamReader(fs1.open(fileStatus1[i].getPath())));
								String line;
								try {
									while ((line = br.readLine()) != null) {
									String[] col = line.split("\t");
			          				String[] columns =col[0].split(",");
			          				DataPointKey dataPoint = new DataPointKey(columns[0],columns[1],columns[2],columns[3]);
									next.add(dataPoint);
									
								}
								}finally {
									br.close();
								}
							}
							}
							/*
							 * Sort both the arraylist prev and next 
							 * as different centroids occur in different part-r files
							 * as doesn't gets added in arraylist in order
							 * so to compare them we have to first sort them
							 */
							Collections.sort(prev);
							Collections.sort(next);

							/*
							 * iterate over both the list to check for convergence
							 */
							Iterator<DataPointKey> it = prev.iterator();
							for (DataPointKey d : next) {
								DataPointKey temp = it.next();
								/*
								 * put a threshold, if distance between the previous and new centroid
								 * is less than one then the centroids converged
								 */
								if ((d.Distance(temp)<=1.00)) {
									IsConverged = true;
									System.out.println(ConvergenceIteration);
									System.out.println();
								} else {
									centroidsConverged = false;
									break;
								}
							}

							
						

						}
						

						jobStatus = (job.waitForCompletion(true) ? true : false);
						return jobStatus;
					
				}

				
		}
			






