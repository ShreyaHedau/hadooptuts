package com.code.shreya.MavenSampleWordCount;

import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
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

public class MovieGenre {

	private static final Log LOG = LogFactory.getLog(MovieGenre.class);

	public static class E_EMapper extends MapReduceBase implements
			Mapper<LongWritable, // Input key Type
					Text, // Input value Type
					Text, // Output key Type
					Text> // Output value Type
	{
		// Map function
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			LOG.info("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$");

			String line = value.toString();
			LOG.info("$$$$$$$$$$$$$$$--key--$$$$$$$$$$$$$$" + key);
			LOG.info("$$$$$$$$$$$$$$$--value--$$$$$$$$$$$$$$" + value);
			LOG.info("$$$$$$$$$$$$$$$--output--$$$$$$$$$$$$$$" + output);
			LOG.info("$$$$$$$$$$$$$$$--line--$$$$$$$$$$$$$$" + line);
            String[] lineFields = value.toString().split(",");
            LOG.info("$$$$$$$$$$$$$$$--vvvvvvvvvvvvvvvvvvvvv--$$$$$$$$$$$$$$" + lineFields[1] + "," + lineFields[2]);
            
			output.collect(new Text(lineFields[1]), new Text(lineFields[2]));
		}
	}

	// Reducer class
	public static class E_EReduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		// Reduce function

		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output,
				Reporter reporter) throws IOException {

			LOG.info("##################################");
			LOG.info("##############-key-###############" + key);
			LOG.info("##############-values-#############" + values);
			LOG.info("##############-output-#############" + output);
			String movieGenere;
			while (values.hasNext()) {
				movieGenere = values.next().toString();
				LOG.info("##############-inside movieGenere-###############" + movieGenere + "sssssssssss" + movieGenere.contains("Comedy"));

				if (movieGenere.contains("Comedy")) {
					LOG.info("##############-inside key-###############" + key);
					output.collect(key, new Text(movieGenere));
				}
			}
		}
	}

	public static void main(String args[]) throws Exception {
		LOG.info("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");

		JobConf conf = new JobConf(MovieGenre.class);
		conf.setJobName("movie_genre");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
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
