import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class Multiplication1_2 {
	// Complete the Matrix1_2_1_Mapper class.
	// Definitely, Generic type (LongWritable, Text, Text, Text) must not be
	// modified
	// Matrix1_2_1 _Mapper class handle the output data from Multiplication1_1 and
	// use 'TextInputFileFormat' as InputFileFormat

	// Optional, you can use both 'setup' and 'cleanup' function, or either of them,
	// or none of them.
	// Optional, you can add and use new methods in this class
	public static class Matrix1_2_1_Mapper extends Mapper<LongWritable, Text, Text, Text> {
		// Optional, Using, Adding, Modifying and Deleting variable is up to you
		int result_columns = 0;
		String newEntry = null;

		// Optional, Utilizing 'setup' function or not is up to you
		protected void setup(Context context) throws IOException, InterruptedException {
			result_columns = context.getConfiguration().getInt("n_third_cols", 0);
		}

		// Definitely, parameter type and name (LongWritable key, Text entry, Context
		// context) must not be modified
		public void map(LongWritable key, Text entry, Context context) throws IOException, InterruptedException {
			// Implement map function.
		}
	}

	// Complete the Matrix1_2_2_Mapper class.
	// Definitely, Generic type (Text, Text, Text, Text) must not be modified
	// Matrix1_2_2 _Mapper class handle the data from Matrix1_2 and use
	// 'KeyValueTextInputFileFormat' as InputFileFormat

	// Optional, you can use both 'setup' and 'cleanup' function, or either of them,
	// or none of them.
	// Optional, you can add and use new methods in this class
	public static class Matrix1_2_2_Mapper extends Mapper<Text, Text, Text, Text> {
		// Optional, Using, Adding, Modifying and Deleting variable is up to you
		int result_rows = 0;
		String newEntry = null;

		// Optional, Utilizing 'setup' function or not is up to you
		protected void setup(Context context) throws IOException, InterruptedException {
			result_rows = context.getConfiguration().getInt("n_first_rows", 0);
		}

		public void map(Text matrix, Text entry, Context context) throws IOException, InterruptedException {
			// Impmelent map function.
		}
	}

	// Complete the Matrix1_2_Reducer class.
	// Definitely, Generic type (Text, Text, Text, Text) must not be modified
	// Definitely, Output format and values must be the same as given sample output

	// Optional, you can use both 'setup' and 'cleanup' function, or either of them,
	// or none of them.
	// Optional, you can add and use new methods in this class
	public static class Matrix1_2_Reducer extends Reducer<Text, Text, Text, Text> {
		// Optional, Using, Adding, Modifying and Deleting variable is up to you
		int n_first_rows = 0;
		int n_second_cols = 0;
		int n_third_cols = 0;

		// Optional, Utilizing 'setup' function or not is up to you
		protected void setup(Context context) throws IOException, InterruptedException {
			n_first_rows = context.getConfiguration().getInt("n_first_rows", 0);
			n_second_cols = context.getConfiguration().getInt("n_second_cols", 0);
			n_third_cols = context.getConfiguration().getInt("n_third_cols", 0);
		}

		// Definitely, parameters type (Text, Iterable<Text>, Context) must not be
		// modified
		// Optional, parameters name (key, values, context) can be modified
		public void reduce(Text entry, Iterable<Text> entryComponents, Context context)
				throws IOException, InterruptedException {
			// Implement reduce function.

		}

	}

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Matrix Multiplication1_1");

		job.setJarByClass(Multiplication1_1.class);
		job.setMapperClass(Multiplication1_1.Matrix1_1_Mapper.class);
		job.setReducerClass(Multiplication1_1.Matrix1_1_Reducer.class);

		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		job.getConfiguration().setInt("n_first_rows", Integer.parseInt(args[3]));
		job.getConfiguration().setInt("n_first_cols", Integer.parseInt(args[4]));
		job.getConfiguration().setInt("n_second_cols", Integer.parseInt(args[5]));

		if (!job.waitForCompletion(true))
			return;

		Configuration conf2 = new Configuration();
		Job job2 = Job.getInstance(conf2, "Matrix Multiplication1_2");

		job2.setJarByClass(Multiplication1_2.class);
		MultipleInputs.addInputPath(job2, new Path(args[2]), TextInputFormat.class, Matrix1_2_1_Mapper.class);
		MultipleInputs.addInputPath(job2, new Path(args[1]), KeyValueTextInputFormat.class, Matrix1_2_2_Mapper.class);
		// job2.setMapperClass(Matrix1_2_1_Mapper.class); // Not needed
		job2.setReducerClass(Matrix1_2_Reducer.class);

		// job2.setInputFormatClass(KeyValueTextInputFormat.class); // Not needed
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		// FileInputFormat.addInputPath(job2, new Path(args[2])); // Not needed
		FileOutputFormat.setOutputPath(job2, new Path(args[2] + "//final"));

		job2.getConfiguration().setInt("n_first_rows", Integer.parseInt(args[3]));
		job2.getConfiguration().setInt("n_second_cols", Integer.parseInt(args[5]));
		job2.getConfiguration().setInt("n_third_cols", Integer.parseInt(args[6]));

		System.exit(job2.waitForCompletion(true) ? 0 : 1);
	}
}
