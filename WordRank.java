package hw1.invertedIndex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordRank {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String inputPath = args[0];
		String metaPath = args[1]+"_meta";
		String outputPath = args[1];
		/**
			Part1-1: MetaData
		**/
		Job job = Job.getInstance(conf, "WordRank");
		job.setJarByClass(WordRank.class);
		
		// set the class of each stage in mapreduce
 		job.setMapperClass(MetaMapper.class);
		job.setCombinerClass(MetaCombiner.class);
		//job.setPartitionerClass(WordRankPartitioner.class);
		job.setSortComparatorClass(WordRankKeyComparator.class); 			
		job.setReducerClass(MetaReducer.class);
		// 										
		// set the output class of Mapper and Reducer
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Term.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// set the number of reducer	
		job.setNumReduceTasks(2);
		// 																												
		// add input/output path
		FileInputFormat.addInputPath(job, new Path(inputPath));				
		FileOutputFormat.setOutputPath(job, new Path(metaPath));
		job.waitForCompletion(true);
		
		/**
			Part1-2: InvertedIndex
		**/
		job = Job.getInstance(conf, "WordRank");
		job.setJarByClass(WordRank.class);
		
		// set the class of each stage in mapreduce
 		job.setMapperClass(InvertedIndexMapper.class);
		job.setCombinerClass(InvertedIndexCombiner.class);
		job.setPartitionerClass(WordRankPartitioner.class);
		job.setSortComparatorClass(WordRankKeyComparator.class); 			
		job.setReducerClass(InvertedIndexReducer.class);
		// 										
		// set the output class of Mapper and Reducer
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(WordValue.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// set the number of reducer	
		job.setNumReduceTasks(2);
		// 																												
		// add input/output path
		FileInputFormat.addInputPath(job, new Path(metaPath));				
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
  }

}

