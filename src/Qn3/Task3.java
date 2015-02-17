package Qn3;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class Task3 extends Configured implements Tool
{	
	private static final String SPACE_SEPARATOR = " ";
	public int run(String[] args) throws Exception {

		//Set up configuration object to put parameter
		Configuration conf = new Configuration();
		if(args.length >= 3){
			conf.set("timestamp", args[2]);
		} else {
			System.out.println("Insufficent args");
			System.out.println("java Task1 <inputfile> <outputfolder> <timestamp>");
			System.exit(0);
		}

		// set output separator to whitespace instead of tab
		conf.set("mapred.textoutputformat.separator", SPACE_SEPARATOR);


		if(args.length >= 5){
			// add stuff for running on hdfs
			conf.addResource(args[3]);
			conf.set("mapred.jar", args[4]);
		}
		
		// delete previously created output folder
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(args[1] + "_Job_1_of_1"), true);

		// add conf object when init Job
		Job job1 = new Job(conf);		
		initJob1(args, job1);

		job1.submit();

		return (job1.waitForCompletion(true) ? 0 : 1);

	}
	private void initJob1(String[] args, Job job1) throws IOException {
		job1.setJobName("Task3-1");
		job1.setJarByClass(Task3.class);

		// set mapper class
		job1.setMapperClass(Mapper3_1.class);

		// set combiner class
		//job.setCombinerClass(Reducer1.class); 

		// set partitioner class
		job1.setMapOutputKeyClass(CompositeKey3.class);
		job1.setPartitionerClass(ActualKeyPartitioner3.class);
		job1.setGroupingComparatorClass(ActualKeyGroupingComparator3.class);
		job1.setSortComparatorClass(CompositeKeyComparator3.class);

		//set reducer class
		job1.setReducerClass(Reducer3_1.class);	

		//set output key and value
		job1.setOutputKeyClass(CompositeKey3.class);
		job1.setOutputValueClass(LongWritable.class);


		// set input and outut format classTask2
		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);


		// set input and output paths
		FileInputFormat.setInputPaths(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1] + "_Job_1_of_1"));
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Configuration(), new Task3(), args));
	}

}
