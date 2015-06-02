package hadoopAuebProject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HistogramDriver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {

		Configuration conf = getConf();
		Job job = Job.getInstance(conf);
		job.setJobName("PowerConsumption Calculator");
		job.setJarByClass(HistogramDriver.class);

		String[] arg0 = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (arg0.length != 2) {
			System.err
					.println("Usage: hadoop jar <my_jar> <input_dir> <output_dir>");
			System.exit(1);
		}

		Path in = new Path(arg0[0]);
		Path out = new Path(arg0[1]);

		FileInputFormat.setInputPaths(job, in);
        FileOutputFormat.setOutputPath(job, out);
        
        
        
		//Defining multiple outputs
		MultipleOutputs.addNamedOutput(job, "month_histogram", TextOutputFormat.class, Text.class, FloatWritable.class);
		MultipleOutputs.addNamedOutput(job, "weekday_histogram", TextOutputFormat.class, Text.class, FloatWritable.class);
		MultipleOutputs.addNamedOutput(job, "timezone_histogram", TextOutputFormat.class, Text.class, FloatWritable.class);
		
		
		

		job.setMapperClass(CountTimeslotMapper.class);
		job.setReducerClass(CountTimesotReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		job.waitForCompletion(true);
		
		return 0;
	}

	public static void main(String[] args) throws Exception {

		int res = ToolRunner.run(new Configuration(), new HistogramDriver(), args);
		System.exit(res);

	}

}
