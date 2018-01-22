package com.code.shreya.MavenSampleWordCount;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ProcessUnits {

	private static final Log LOG = LogFactory.getLog(ProcessUnits.class);

	public static class E_EMapper extends MapReduceBase implements
			Mapper<LongWritable, // Input key Type
					Text, // Input value Type
					Text, // Output key Type
					IntWritable> // Output value Type
	{
		// Map function
		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			
			LOG.info("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$");

			String line = value.toString();
			LOG.info("$$$$$$$$$$$$$$$--key--$$$$$$$$$$$$$$" + key);
			LOG.info("$$$$$$$$$$$$$$$--value--$$$$$$$$$$$$$$" + value);
			LOG.info("$$$$$$$$$$$$$$$--output--$$$$$$$$$$$$$$" + output);
			LOG.info("$$$$$$$$$$$$$$$--line--$$$$$$$$$$$$$$" + reporter);
			if (line != null)
				line = line.trim();
			String lasttoken = null;
			StringTokenizer s = new StringTokenizer(line, "\t");
			String year = s.nextToken();
			LOG.info("$$$$$$$$$$$$$$$--year--$$$$$$$$$$$$$$" + year);

			// check for null in year
			if (year != null)
				year = year.trim();
			while (s.hasMoreTokens()) {
				lasttoken = s.nextToken();
			}
			// check for null
			if (lasttoken != null) {
				lasttoken = lasttoken.trim();
				LOG.info("$$$$$$$$$$$$$$$--lasttoken--$$$$$$$$$$$$$$" + lasttoken);

				int avgprice = Integer.parseInt(lasttoken);
				output.collect(new Text(year), new IntWritable(avgprice));
				LOG.info(year + "-----> " + avgprice);
			}
			
		}
	}

	// Reducer class
	public static class E_EReduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
		// Reduce function
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output,
				Reporter reporter) throws IOException {
		
			LOG.info("##################################");
			LOG.info("##############-key-###############" + key);
			LOG.info("##############-values-#############" + values);
			LOG.info("##############-output-#############" + output);

			int maxavg = 30;
			int val = Integer.MIN_VALUE;
		
			while (values.hasNext()) {
				if ((val = values.next().get()) > maxavg) {
					LOG.info("##############-inside key-###############" + key);
					output.collect(key, new IntWritable(val));
				}
			}
		}
	}

	public static void main(String args[]) throws Exception {
		LOG.info("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");

		JobConf conf = new JobConf(ProcessUnits.class);
		conf.setJobName("max_eletricityunits");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		conf.setMapperClass(E_EMapper.class);
		conf.setCombinerClass(E_EReduce.class);
		conf.setReducerClass(E_EReduce.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		LOG.info("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
		JobClient.runJob(conf);
	}

}
