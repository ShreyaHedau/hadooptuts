package com.code.shreya.MavenSampleWordCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class PartitionerExample extends Configured implements Tool {

	public static final Log LOG = LogFactory.getLog(PartitionerExample.class);

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context) {
			try {
				String[] str = value.toString().split("\t", -3);
				String gender = str[3];
				LOG.info("@@@@@@@@@@@--Gender--@@@@@@@@@@@@@@@@@" + gender);
				LOG.info("@@@@@@@@@@@--value--@@@@@@@@@@@@@@@@@" + value);
				context.write(new Text(gender), new Text(value));
			} catch (Exception e) {
				System.out.println(e.getMessage());
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, IntWritable> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			int max = -1;

			for (Text val : values) {

				LOG.info("#############--val--#################" + val);
				String[] str = val.toString().split("\t", -3);
				LOG.info("#############--str--#################" + str);
				if (Integer.parseInt(str[4]) > max)
					max = Integer.parseInt(str[4]);
			}

			context.write(new Text(key), new IntWritable(max));
		}
	}

	public static class CarderPartioner extends Partitioner<Text, Text> {

		@Override
		public int getPartition(Text key, Text value, int numReduceTasks) {
			String[] str = value.toString().split("\t");
			int age = Integer.parseInt(str[2]);
			LOG.info("$$$$$$$$$$$$$$--str--$$$$$$$$$$$$$$$$$$$" + str);
			LOG.info("$$$$$$$$$$$$$$--age--$$$$$$$$$$$$$$$$$$$" + age);
			LOG.info("$$$$$$$$$$$$$$--numReduceTasks--$$$$$$$$$$$$$$$$$$$" + numReduceTasks);
			if (numReduceTasks == 0) {
				return 0;
			}
			if (age <= 20) {
				return 0;
			} else if (age > 20 && age <= 30) {
				return 1 % numReduceTasks;
			} else if(age > 30 && age <= 60){
				return 2 % numReduceTasks;
			}
			else
			{
				return 3 % numReduceTasks;
			}
		}

	}

	public int run(String[] arg) throws Exception {

		Configuration conf = getConf();
		Job job = new Job(conf, "PartionerExample");

		job.setJarByClass(PartitionerExample.class);

		FileInputFormat.setInputPaths(job, new Path(arg[0]));
		FileOutputFormat.setOutputPath(job, new Path(arg[1]));

		job.setMapperClass(Map.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// set partitioner statement

		job.setPartitionerClass(CarderPartioner.class);

		job.setReducerClass(Reduce.class);
		job.setNumReduceTasks(4);
		job.setInputFormatClass(TextInputFormat.class);

		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;
	}

	public static void main(String args[]) throws Exception {
		System.out.println("inside Partitioner Example");
		int res = ToolRunner.run(new Configuration(), new PartitionerExample(), args);
		System.exit(0);
	}
}
