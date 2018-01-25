package mba;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;


public class MBADriver{

	public static void main(String[] args) throws Exception {

			JobConf job1 = new JobConf(MBADriver.class);
			job1.setJobName("SetFormation");
			
			job1.setMapOutputKeyClass(Text.class);
			job1.setMapOutputValueClass(IntWritable.class);
			
		    job1.setOutputKeyClass(Text.class);
			job1.setOutputValueClass(Text.class);
			
		    job1.setMapperClass(MBAPMapper.class);
			job1.setReducerClass(MBAPReducer.class);
			job1.setPartitionerClass(MBAPPartitioner.class);
		    
			job1.setNumReduceTasks(4);
		    
		    job1.setInputFormat(TextInputFormat.class);
		    job1.setOutputFormat(TextOutputFormat.class);
		    FileInputFormat.setInputPaths(job1, new Path("MBA"));
		    FileOutputFormat.setOutputPath(job1, new Path("mbaoutput"));
	    
	    JobConf job2 = new JobConf(MBADriver.class);
			job2.setJobName("Association");
			
			job2.setMapOutputKeyClass(Text.class);
		    job2.setMapOutputValueClass(Text.class);

		    job2.setOutputKeyClass(Text.class);
		    job2.setOutputValueClass(FloatWritable.class);
		    
		    job2.setMapperClass(MBAMapper2.class);
		    job2.setReducerClass(MBAReducer2.class);
		    job2.setPartitionerClass(MBAPPartitioner2.class);
		    job2.setNumReduceTasks(3);
		    
		    job2.setInputFormat(TextInputFormat.class);
		    job2.setOutputFormat(TextOutputFormat.class);
		    FileInputFormat.setInputPaths(job2, new Path("mbaoutput"));
		    FileOutputFormat.setOutputPath(job2, new Path("mbaout"));	    		    
			JobClient.runJob(job1);
			JobClient.runJob(job2);						
	}	
}