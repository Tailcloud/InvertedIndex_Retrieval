package hw1.retrieval;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Retrieval {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String inputPath = args[0];
		String metaPath = args[1]+"_meta";
		String outputPath = args[1];
		conf.set("filePath", args[2]);
		conf.set("cmd", args[3]);
		/**
			Part2-1: calculate term weighting of needed files
		**/
		Job job = Job.getInstance(conf, "Retrieval");
		job.setJarByClass(Retrieval.class);
		
		// set the class of each stage in mapreduce
 		job.setMapperClass(RetrievalMetaMapper.class);		
		job.setReducerClass(RetrievalMetaReducer.class);
											
		// set the output class of Mapper and Reducer
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(TermWeighting.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// set the number of reducer	
		job.setNumReduceTasks(1);
																							
		// add input/output path
		FileInputFormat.addInputPath(job, new Path(inputPath));				
		FileOutputFormat.setOutputPath(job, new Path(metaPath));
		
		job.waitForCompletion(true);
		
		/**
			Part2-2: calculate and output 10 highest score files
		**/
		job = Job.getInstance(conf, "Retrieval");
		job.setJarByClass(Retrieval.class);
		
		// set the class of each stage in mapreduce
 		job.setMapperClass(RetrievalMapper.class);		
		job.setReducerClass(RetrievalReducer.class);
									
		// set the output class of Mapper and Reducer
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FileScore.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// set the number of reducer
		job.setNumReduceTasks(1);
																						
		// add input/output path
		FileInputFormat.addInputPath(job, new Path(metaPath));				
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

}