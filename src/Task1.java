import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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
		// add conf object when init Job
		Job job = new Job(conf);
		
		job.setJobName("Task1");
		job.setJarByClass(Task1.class);		
		
		// set mapper class
		job.setMapperClass(Mapper1.class);
		
		// set combiner class
		//job.setCombinerClass(Reducer1.class); 
		
		// set partitioner class
		job.setMapOutputKeyClass(CompositeKey.class);
		job.setPartitionerClass(ActualKeyPartitioner.class);
		job.setGroupingComparatorClass(ActualKeyGroupingComparator.class);
		job.setSortComparatorClass(CompositeKeyComparator.class);
		
		//set reducer class
		job.setReducerClass(Reducer1.class);	
		
		//set output key and value
		job.setOutputKeyClass(CompositeKey.class);
		job.setOutputValueClass(LongWritable.class);

		
		// set input and outut format class
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		
		// set input and output paths
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		
		job.submit();
		return (job.waitForCompletion(true) ? 0 : 1);
	}
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Configuration(), new Task1(), args));
	}

}
