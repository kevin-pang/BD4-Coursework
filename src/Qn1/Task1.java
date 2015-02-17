package Qn1;
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

		// add largestId to conf
		if(args.length >= 7){
			conf.setInt("largestId",Integer.parseInt(args[6]));
		} else{
			// id in "enwiki-20080103-perftest.txt"
			// ranges from 1 to 15071250
			conf.setInt("largestId",15071250);
		}

		// delete previously created output folder
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(args[1] + "_Job_1_of_1"), true);


		// add conf object when init Job
		Job job = new Job(conf);


		job.setJobName("Task1");
		job.setJarByClass(Task1.class);		

		// set mapper class
		job.setMapperClass(Mapper1.class);

		// set combiner class
		//job.setCombinerClass(Reducer1.class); 

		// set partitioner class
		job.setMapOutputKeyClass(CompositeKey1.class);
		job.setPartitionerClass(ActualKeyPartitioner1.class);
		job.setGroupingComparatorClass(ActualKeyGroupingComparator1.class);
		job.setSortComparatorClass(CompositeKeyComparator1.class);

		//set reducer class
		job.setReducerClass(Reducer1.class);	

		//set output key and value
		job.setOutputKeyClass(CompositeKey1.class);
		job.setOutputValueClass(LongWritable.class);


		// set input and outut format class
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);


		// set input and output paths
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1] + "_Job_1_of_1"));

		job.submit();
		return (job.waitForCompletion(true) ? 0 : 1);
	}
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Configuration(), new Task1(), args));
	}

}
