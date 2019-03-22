package com.stone;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ETLMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
	
	private Text k = new Text();
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		// 1 获取一行
		String line = value.toString();
		
		// 2 清洗数据
		String etlStr = ETLUtil.etlStr(line);
		if (StringUtils.isBlank(etlStr)) {
			return;
		}
		k.set(etlStr);
		
		// 3 写出
		context.write(k, NullWritable.get());
	}

}
