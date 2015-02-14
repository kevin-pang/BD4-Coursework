package Qn2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.LongWritable.DecreasingComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class Task2 extends Configured implements Tool {
	public int run(String[] args) throws Exception {

		//Set up configuration object to put parameter
		Configuration conf = new Configuration();
		if(args.length >= 5){
			conf.set("start", args[2]);
			conf.set("end", args[3]);
			conf.set("k", args[4]);
		} else {
			System.out.println("Insufficent args");
			System.out.println("java Task1 <inputfile> <outputfolder> <start> <end> <k>");
			System.exit(0);
		}
		// add conf object when init Job
		Job job1 = new Job(conf);		
		initJob1(args, job1);
		
		Job job2 = new Job(conf);
		initJob2(args, job2);
		
		
		job1.submit();
		
		//wait for job1 completion before starting job2
		if (job1.waitForCompletion(true)) {
			
			job2.submit();
	       
	    }
		 return (job2.waitForCompletion(true) ? 0 : 1);
		
	}
	private void initJob1(String[] args, Job job1) throws IOException {
		job1.setJobName("Task2-1");
		job1.setJarByClass(Task2.class);
		
		// set mapper class
		job1.setMapperClass(Mapper2_1.class);
		
		// set combiner class
		//job.setCombinerClass(Reducer1.class); 
		
		//set reducer class
		job1.setReducerClass(Reducer2_1.class);	
		
		//set output key and value
		job1.setOutputKeyClass(LongWritable.class);
		job1.setOutputValueClass(LongWritable.class);

		
		// set input and outut format classTask2
		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);
		
		
		// set input and output paths
		FileInputFormat.setInputPaths(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1] + "_job_1_of_2"));
	}
	
	private void initJob2(String[] args, Job job2) throws IOException {
		job2.setJobName("Task2-2");
		job2.setJarByClass(Task2.class);
		
		// set sort order decreasing
		//job2.setSortComparatorClass(DecreasingComparator.class);
		
		// set mapper class
		job2.setMapperClass(Mapper2_2.class);
		
		// set combiner class
		//job.setCombinerClass(Reducer1.class); 
		
		// set partitioner class
		job2.setMapOutputKeyClass(CompositeKey2.class);
		job2.setPartitionerClass(ActualKeyPartitioner2.class);
		job2.setGroupingComparatorClass(ActualKeyGroupingComparator2.class);
		job2.setSortComparatorClass(CompositeKeyComparator2.class);
		
		//set reducer class
		job2.setReducerClass(Reducer2_2.class);	
		
		//set output key and value
		job2.setOutputKeyClass(LongWritable.class);
		job2.setOutputValueClass(LongWritable.class);

		
		// set input and outut format class
		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
		
		
		// set input and output paths
		FileInputFormat.setInputPaths(job2, new Path(args[1] + "_job_1_of_2/part-r-00000"));
		FileOutputFormat.setOutputPath(job2, new Path(args[1] + "_job_2_of_2"));
	}
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Configuration(), new Task2(), args));
	}

}
