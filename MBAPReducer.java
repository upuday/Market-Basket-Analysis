package mba;


import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class MBAPReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, Text> {

	@Override
	public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		
		int sum = 0;
		int min_support = 10;
		String input[]=key.toString().replaceAll("\\[","").replaceAll("\\]","").split(",");
	    while (values.hasNext()) {
	    	sum += values.next().get();	
	    	
	    }
	    if(input.length==1)
	    {
	    	if(sum > min_support)
	    	output.collect(key, new Text(Integer.toString(sum)));
	    }
	   
	    if(input.length==2){
	    if(!(input[0].trim()).equals(input[1].trim()))
	    {
	    	if(sum > min_support)
	        output.collect(key, new Text(Integer.toString(sum)));
	    }
	}
	    else if(input.length==3)
	    {
	    	if(!(input[0].trim()).equals(input[1].trim()))
	    	{
		    if(!(input[1].trim()).equals(input[2].trim()))
		    {
		    	if(sum > min_support)
		    	output.collect(key, new Text(Integer.toString(sum)));
		    }
	    	}
	    }
	    else if(input.length==4)
	    {
	    	if(!(input[0].trim()).equals(input[1].trim()))
	    	{
		    if(!(input[1].trim()).equals(input[2].trim()))
		    {
		    	if(!(input[2].trim()).equals(input[3].trim()))
		    	{
		    		if(sum > min_support)
		    		output.collect(key, new Text(Integer.toString(sum)));
		    	}
		    }
	    	}
	    }
	}
}

