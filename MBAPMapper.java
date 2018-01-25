package mba;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;



public class MBAPMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
	
	private Text itemset = new Text();
	
	public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {
		
		String transaction[] = value.toString().split(" ");
		ArrayList<String> initialset = new ArrayList<String>();
		for(int i=0; i<7 ;i++)
		initialset.add(transaction[i]);
		Collections.sort(initialset);
		String [] set = initialset.toArray(new String[initialset.size()]);
		List<String> itemsets = getItemSets(set);
		
		for(String itmset : itemsets){
				itemset.set(itmset.replaceAll(" ", ""));
				output.collect(itemset, new IntWritable(1));
		}			
	}
	
	private List<String> getItemSets(String[] items) {
				
		List<String> itemsets = new ArrayList<String>();	
				
		int n = items.length;
				
		int[] masks = new int[n];
				
		for (int i = 0; i < n; i++)
			masks[i] = (1<<i);
				
		for (int i = 0; i < (1<<n); i++){				
					
			List<String> newList = new ArrayList<String>(n);
					
	
			for (int j = 0; j < n; j++){
			
				if ((masks[j] & i) != 0){ 
					
					     newList.add(items[j]);
						
					}
		    
		               
		       if(j == n-1 && newList.size() > 0 && newList.size() < 5){
		       	itemsets.add(newList.toString());
		       	
		       }
			}
		}
		       			
		return itemsets;
	}

}
