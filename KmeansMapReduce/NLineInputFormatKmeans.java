import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class NLineInputFormatKmeans {

	//array to hold the data points that is read from the data
	//file in the distributed cache
	public static String arr[][] = new String[9963][3];
	public static String DATA_FILE = "/DataFile.csv";
	public static String INPUT_FILE_NAME = "/NLineInput.csv";

	public static class MapperNLineInputFormat extends
			Mapper<Object, Text, Text, Text> {

		@Override
		protected void setup(Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			try {
				// Get centroid from Distributed cache
				Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
				if (cacheFiles != null && cacheFiles.length > 0) {
					String line;
					/*
					 * Read the data file from the distributed cache and put 
					 * each data point in an array
					 */
					int r = 0;
					BufferedReader br = new BufferedReader(new FileReader(
							cacheFiles[0].toString()));
					try {
						while ((line = br.readLine()) != null) {
							String[] x = line.split(",");
							int j = 0;
							for (int i = 0; i < 9; i++) {
								if (i == 1 || i == 7 || i == 8) {
									String aur = x[i];
									if (aur == "")
										arr[r][j] = "0";
									else
										arr[r][j] = aur;
									j++;
								}
							}
							r++;
						}

					} finally {
						br.close();
					}
				}
			} catch (IOException e) {
				System.err.println("IO Exception - Distributed cache: "
						+ e.getMessage());
			}


		}

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			/*
			 * the value in map is the number of clusters and
			 * initial configuration
			 */
			String columns[] = value.toString().split(",");
			int k = Integer.parseInt(columns[0]);
			int initial = Integer.parseInt(columns[1]);
			//pass the value of k and initial configuration to the Kmeans function
			String clustering = Kmeans(arr, k, initial);
			Text val = new Text(clustering);
			context.write(value, val);
		}

	}// end of mapper class

	public static String Kmeans(String[][] newarr, int nc, int inFile) {
		int r = 9963;
		/*
		 * the xtest the array from which to choose the row
		 * number for initial configuration
		 */
		int xtest[] = new int[] { 775, 1020, 200, 127, 329, 1626, 1515, 651,
				658, 328, 1160, 108, 422, 88, 105, 261, 212, 1941, 1724, 704,
				1469, 635, 867, 1187, 445, 222, 1283, 1288, 1766, 1168, 566,
				1812, 214, 53, 423, 50, 705, 1284, 1356, 996, 1084, 1956, 254,
				711, 1997, 1378, 827, 1875, 424, 1790, 633, 208, 1670, 1517,
				1902, 1476, 1716, 1709, 264, 1, 371, 758, 332, 542, 672, 483,
				65, 92, 400, 1079, 1281, 145, 1410, 664, 155, 166, 1900, 1134,
				1462, 954, 1818, 1679, 832, 1627, 1760, 1330, 913, 234, 1635,
				1078, 640, 833, 392, 1425, 610, 1353, 1772, 908, 1964, 1260,
				784, 520, 1363, 544, 426, 1146, 987, 612, 1685, 1121, 1740,
				287, 1383, 1923, 1665, 19, 1239, 251, 309, 245, 384, 1306, 786,
				1814, 7, 1203, 1068, 1493, 859, 233, 1846, 1119, 469, 1869,
				609, 385, 1182, 1949, 1622, 719, 643, 1692, 1389, 120, 1034,
				805, 266, 339, 826, 530, 1173, 802, 1495, 504, 1241, 427, 1555,
				1597, 692, 178, 774, 1623, 1641, 661, 1242, 1757, 553, 1377,
				1419, 306, 1838, 211, 356, 541, 1455, 741, 583, 1464, 209,
				1615, 475, 1903, 555, 1046, 379, 1938, 417, 1747, 342, 1148,
				1697, 1785, 298, 1485, 945, 1097, 207, 857, 1758, 1390, 172,
				587, 455, 1690, 1277, 345, 1166, 1367, 1858, 1427, 1434, 953,
				1992, 1140, 137, 64, 1448, 991, 1312, 1628, 167, 1042, 1887,
				1825, 249, 240, 524, 1098, 311, 337, 220, 1913, 727, 1659,
				1321, 130, 1904, 561, 1270, 1250, 613, 152, 1440, 473, 1834,
				1387, 1656, 1028, 1106, 829, 1591, 1699, 1674, 947, 77, 468,
				997, 611, 1776, 123, 979, 1471, 1300, 1007, 1443, 164, 1881,
				1935, 280, 442, 1588, 1033, 79, 1686, 854, 257, 1460, 1380,
				495, 1701, 1611, 804, 1609, 975, 1181, 582, 816, 1770, 663,
				737, 1810, 523, 1243, 944, 1959, 78, 675, 135, 1381, 1472 };

		int itr = 0;

		// add one more condition of no change , while outside
		HashMap<Integer, String> centroids = new HashMap<Integer, String>();
		HashMap<Integer, ArrayList<String>> clusters = new HashMap<Integer, ArrayList<String>>();
		boolean stillKeepMoving = true;
		// for initial random points as centroids
		for (int f = 0; f < nc; f++) {
			String temp = "";
			int row = xtest[inFile];
			for (int s = 0; s < 3; s++)
				temp = temp + newarr[row - 1][s] + ",";
			centroids.put(f + 1, temp);

			inFile++;
		}

		// initially assign clusters
		for (int row = 0; row < r; row++) {
			double minimum = 10000; // The minimum value to beat.
			int cluster = 0;
			String datapoint = "";
			for (Entry t : centroids.entrySet()) {
				double distance = 0.0;
				double diff = 0.0;
				datapoint = "";
				String split = (String) t.getValue();
				String centroidarr[] = split.split(",");
				for (int col = 0; col < 3; col++) {

					datapoint = datapoint + newarr[row][col] + ",";

					double temp1 = Double.parseDouble(newarr[row][col]);
					double temp2 = Double.parseDouble(centroidarr[col]);
					diff = diff + ((temp1 - temp2) * (temp1 - temp2));
				}
				distance = Math.sqrt(diff);
				if (distance < minimum) {
					minimum = distance;
					cluster = (Integer) t.getKey();
				}

			}
			newarr[row][3] = Integer.toString(cluster);
			ArrayList<String> values = clusters.get(cluster);
			if (values == null) {
				values = new ArrayList<String>();

			}
			values.add(datapoint);

			clusters.put(cluster, values);
		} // end of while for all datapoints

		// code for new centroids

		while (stillKeepMoving) {
			System.out.println("in while " + itr + " " + nc);
			centroids.clear();
			for (Entry clust : clusters.entrySet()) {
				ArrayList<String> points = (ArrayList<String>) clust.getValue();
				int count = 0;
				for (String n : points) {
					count++;
				}
				HashMap<Integer, Double> sumarr = new HashMap<Integer, Double>();
				for (String p : points) {
					String[] datapts = new String[3];
					datapts = p.split(",");

					double datap = 0.0;
					for (int pd = 0; pd < datapts.length; pd++) {
						if (!datapts[pd].isEmpty()) {
							datap = Double.parseDouble(datapts[pd]);
							if (sumarr.get(pd) == null)
								sumarr.put(pd, datap);
							else {
								double previousSum = sumarr.get(pd);
								sumarr.put(pd, (previousSum + datap));
							}

						}

					}
				}
				String newcentroid = "";
				for (Entry e : sumarr.entrySet()) {
					double valueInMap = (Double) e.getValue();
					double avg = valueInMap / count;
					newcentroid = newcentroid + Double.toString(avg) + ",";
				}
				centroids.put((Integer) clust.getKey(), newcentroid);
			}

			// to make sure KMeans has converged or not
			stillKeepMoving = false;

			clusters.clear();
			for (int m = 0; m < r; m++) {
				double minimum = 10000; // The minimum value to beat.
				int cluster = 0;
				String datapoint = "";
				for (Entry t : centroids.entrySet()) {
					double distance = 0.0;
					double diff = 0.0;
					datapoint = "";
					String split = (String) t.getValue();
					String centroidarr[] = split.split(",");
					for (int col = 0; col < 3; col++) {
						if (col < 2)
							datapoint = datapoint + newarr[m][col] + ",";
						else
							datapoint = datapoint + newarr[m][col] + ",";

						double temp1 = Double.parseDouble(newarr[m][col]);
						double temp2 = Double.parseDouble(centroidarr[col]);
						diff = diff + ((temp1 - temp2) * (temp1 - temp2));
					}
					distance = Math.sqrt(diff);
					if (distance < minimum) {
						minimum = distance;
						cluster = (Integer) t.getKey();
					}

				}
				int clusterinRow = Integer.valueOf(newarr[m][3]);
				if (clusterinRow != cluster) {
					stillKeepMoving = true;
					newarr[m][3] = Integer.toString(cluster);
					ArrayList<String> values = clusters.get(cluster);
					if (values == null) {
						values = new ArrayList<String>();

					}

					values.add(datapoint);

					clusters.put(cluster, values);

				} else {
					ArrayList<String> values = clusters.get(cluster);
					if (values == null) {
						values = new ArrayList<String>();

					}

					values.add(datapoint);

					clusters.put(cluster, values);

				}

			}
			itr++;
		}// end of while loop
		if (stillKeepMoving == false)
			System.out.println("converged");
		System.out.println(clusters.size());
		String result = "";
		for (Entry e : clusters.entrySet()) {
			int countmem = 0;
			String cent = centroids.get(e.getKey());
			String[] centarr = cent.split(",");
			ArrayList<String> datainCluster = (ArrayList<String>) e.getValue();
			for (String p : datainCluster) {
				countmem++;

			}
			result = result + countmem + " ";

		}

		return result;
	}

	public static void main(String[] args) throws Exception {
		run(args);
	}

	public static void run(String[] args) throws Exception {
	String	IN = args[0];
		String input = IN;
		if (args.length != 2) {
			System.out
					.println("Two parameters are required for DriverNLineInputFormat- <input dir> <output dir>\n");
			System.exit(0);
		}
		Configuration conf = new Configuration();
		Job job = new Job(conf, "kmean");
		job.setJarByClass(NLineInputFormatKmeans.class);
		//add data file to distributed cache
		Path hdfsPath = new Path(input + DATA_FILE);
		DistributedCache.addCacheFile(hdfsPath.toUri(), job.getConfiguration());

		job.setInputFormatClass(NLineInputFormat.class);
		Path localpath = new Path(input + INPUT_FILE_NAME);
		NLineInputFormat.addInputPath(job, localpath);
		//set number of line to be read from the parameter file = 4
		job.getConfiguration().setInt(
				"mapreduce.input.lineinputformat.linespermap", 4);

		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(MapperNLineInputFormat.class);
		//as map only job set number of reduce task zero
		job.setNumReduceTasks(0);

		boolean success = job.waitForCompletion(true);
		System.exit(success == true ? 0 : 1);

	}

}
