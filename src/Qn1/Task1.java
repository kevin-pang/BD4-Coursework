package Qn1;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;




public class Task1 extends Configured implements Tool {
	private static final String SPACE_SEPARATOR = " ";
	public int run(String[] args) throws Exception {

		//Set up configuration object to put parameter
		Configuration conf = new Configuration();
		if(args.length >= 4){
			conf.set("start", args[2]);
			conf.set("end", args[3]);
		} else {
			System.out.println("Insufficent args");
			System.out.println("java Task1 <inputfile> <outputfolder> <start> <end>");
			System.exit(0);
		}

		// set output separator to whitespace instead of tab
		conf.set("mapred.textoutputformat.separator", SPACE_SEPARATOR);

		if(args.length >= 6){
			// add stuff for running on hdfs
			conf.addResource(args[4]);
			conf.set("mapred.jar", args[5]);
		}		

		// delete previously created output folder
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(args[1] + "_Job_1_of_2"), true);
		fs.delete(new Path(args[1] + "_partitions.lst"), false);
		fs.delete(new Path(args[1] + "_Job_2_of_2"), true);

		// add conf object when init Job
		Job job1 = new Job(conf); 

		initJob1(args, job1);
		job1.submit();
		
		Job job2 = new Job(conf);
		//wait for job1 completion before starting job2
		if (job1.waitForCompletion(true)) {

			initJob2(args, job2);

			job2.submit();

		}
		return (job2.waitForCompletion(true) ? 0 : 1);
	}
	
	private void initJob1(String[] args, Job job1) throws IOException {
		job1.setJobName("Task1-1(R-all)-Final");
		job1.setJarByClass(Task1.class);		

		// set mapper class
		job1.setMapperClass(Mapper1.class);

		// settings for secondary sort
		job1.setMapOutputKeyClass(CompositeKey1.class);
		job1.setMapOutputValueClass(LongWritable.class);
		job1.setPartitionerClass(ActualKeyPartitioner1.class);
		job1.setGroupingComparatorClass(ActualKeyGroupingComparator1.class);
		job1.setSortComparatorClass(CompositeKeyComparator1.class);

		//set reducer class
		job1.setReducerClass(Reducer1.class);	

		//set output key and value
		job1.setOutputKeyClass(LongWritable.class);
		job1.setOutputValueClass(Text.class);

		// set input and outut format class
		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(SequenceFileOutputFormat.class);

		// set input and output paths
		FileInputFormat.setInputPaths(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1] + "_Job_1_of_2"));
	}
	
	private void initJob2(String[] args, Job job2) throws IOException, ClassNotFoundException, InterruptedException {
		job2.setJobName("Task1-2(R-all)-Final");
		job2.setJarByClass(Task1.class);

		// Set number of reducer using 5th arg
		if(args.length >= 7){
			job2.setNumReduceTasks(Integer.parseInt(args[6]));
		}

		// TotalOrderPartitioner Settings
		job2.setPartitionerClass(TotalOrderPartitioner.class);
		TotalOrderPartitioner.setPartitionFile(job2.getConfiguration(), new Path(args[1] + "_partitions.lst"));


		// set input and outut format classTask2
		job2.setInputFormatClass(SequenceFileInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);


		// set input and output paths
		FileInputFormat.setInputPaths(job2, new Path(args[1] + "_Job_1_of_2"));
		FileOutputFormat.setOutputPath(job2, new Path(args[1] + "_Job_2_of_2"));

		// Input sampler to write partition file using Random Sampler
		InputSampler.writePartitionFile(job2, new InputSampler.RandomSampler(.01, 100000));
	}
	
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Configuration(), new Task1(), args));
	}

}
