package com.jpmc.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LzoSampleMapper extends
		Mapper<LongWritable, Text, NullWritable, Text> {

	String[] tokens = null;
	StringBuffer outputString;
	Text outputValue = new Text();
	NullWritable nwKey = NullWritable.get();
	static final String RECDELIMETER = "|";

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		if (value == null || value.toString().length() == 0) {
			return;
		}
		
		tokens = value.toString().split("[|]");
		if (tokens.length >= 5) {
			outputString = new StringBuffer();
			outputString.append(tokens[0]).append(RECDELIMETER).append(tokens[1])
				.append(RECDELIMETER).append(tokens[2]).append(RECDELIMETER)
				.append(tokens[3]).append(RECDELIMETER).append(tokens[4]);
		}
		else
		{
			return;
		}
		outputValue.set(outputString.toString());
		context.write(nwKey, outputValue);
	}

}
