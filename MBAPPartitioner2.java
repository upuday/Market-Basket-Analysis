package mba;


import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

public class MBAPPartitioner2 implements Partitioner<Text, Text> {
	
	@Override
	public int getPartition(Text key, Text value, int numReduceTasks) {
		
		String s = key.toString().replace("[", "").replace("]", "");
		int keySize = s.split(",").length;
		if(keySize == 1)
			return 0;
		else if(keySize == 2)
			return 1;
		else 
			return 2;
	}

	@Override
	public void configure(JobConf arg0) {
		// TODO Auto-generated method stub
		
	}


}