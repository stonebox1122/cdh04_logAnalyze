package com.stone;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ETLDriver {

	public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		args = new String[] { "d:/code/inputetl", "d:/code/output" };

		// 1 获取配置信息，或者job对象实例
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration);

		// 2、设置jar的加载路径
		job.setJarByClass(ETLDriver.class);

		// 3、设置map和reduce类
		job.setMapperClass(ETLMapper.class);

		// 4、设置map输出的key和value类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);

		// 5、设置最终输出的key和value类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		// 6、设置输入和输出路径
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// 因为这里我们不使用Reduce
		job.setNumReduceTasks(0);

		// 7、提交job
		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);

	}

}
