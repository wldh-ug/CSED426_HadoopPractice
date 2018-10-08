import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class Multiplication1_1 {

	// Complete the Matrix1_1_Mapper class.
	// Definitely, Generic type (Text, Text, Text, Text) must not be modified

	// Optional, you can add and use new methods in this class
	// Optional, you can use both 'setup' and 'cleanup' function, or either of them,
	// or none of them.

	public static class Matrix1_1_Mapper extends Mapper<Text, Text, Text, Text> {
		// Optional, Using, Adding, Modifying and Deleting variable is up to you
		int n_first_rows = 0;
		int n_second_cols = 0;

		// Optional, Utilizing 'setup' function or not is up to you
		protected void setup(Context context) throws IOException, InterruptedException {
			n_first_rows = context.getConfiguration().getInt("n_first_rows", 0);
			n_second_cols = context.getConfiguration().getInt("n_second_cols", 0);
		}

		// Definitely, parameter type and name (Text matrix, Text entry, Context
		// context) must not be modified
		public void map(Text matrix, Text entry, Context context)
				throws IOException, InterruptedException {
			// Implement map function.

			String[] record = entry.toString().split(",");

			// NOTE: According to TA, names of matrices is fixed as "a" and "b"
			if (matrix.toString().equals("a")) {

				for (int k = 0; k < n_second_cols; k++) {

					context.write(new Text(record[0] + "," + k),
							new Text(record[1] + "," + record[2]));

				}

			} else {

				for (int k = 0; k < n_first_rows; k++) {

					context.write(new Text(k + "," + record[1]),
							new Text(record[0] + "," + record[2]));

				}

			}

		}

	}

	// Complete the Matrix1_1_Reducer class.
	// Definitely, Generic type (Text, Text, Text, Text) must not be modified
	// Definitely, Output format and values must be the same as given sample output

	// Optional, you can use both 'setup' and 'cleanup' function, or either of them,
	// or none of them.
	// Optional, you can add and use new methods in this class
	public static class Matrix1_1_Reducer extends Reducer<Text, Text, Text, Text> {
		// Optional, Using, Adding, Modifying and Deleting variable is up to you
		int n_first_cols = 0;

		// Optional, Utilizing 'setup' function or not is up to you
		protected void setup(Context context) throws IOException, InterruptedException {
			n_first_cols = context.getConfiguration().getInt("n_first_cols", 0);
		}

		// Definitely, parameters type (Text, Iterable<Text>, Context) must not be
		// modified
		// Optional, parameters name (key, values, context) can be modified
		public void reduce(Text entry, Iterable<Text> entryComponents, Context context)
				throws IOException, InterruptedException {
			// Implement reduce function.

			int result = 0;
			int[] partialResult = new int[n_first_cols];
			boolean[] partialAssigned = new boolean[n_first_cols];
			boolean[] partialCalculated = new boolean[n_first_cols];

			for (Text rawValue : entryComponents) {

				String[] value = rawValue.toString().split(",");
				int partialIndex = Integer.parseInt(value[0]);

				if (partialAssigned[partialIndex]) {

					partialResult[partialIndex] *= Integer.parseInt(value[1]);
					partialCalculated[partialIndex] = true;

				} else {

					partialResult[partialIndex] = Integer.parseInt(value[1]);
					partialAssigned[partialIndex] = true;

				}

			}

			for (int i = 0; i < n_first_cols; i++) {

				if (partialCalculated[i]) {

					result += partialResult[i];

				}

			}

			context.write(entry, new Text(Integer.toString(result)));

		}

	}

	// Definitely, Main function must not be modified
	public static void main(String[] args)
			throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Matrix Multiplication1_1");

		job.setJarByClass(Multiplication1_1.class);
		job.setMapperClass(Matrix1_1_Mapper.class);
		job.setReducerClass(Matrix1_1_Reducer.class);

		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.getConfiguration().setInt("n_first_rows", Integer.parseInt(args[2]));
		job.getConfiguration().setInt("n_first_cols", Integer.parseInt(args[3]));
		job.getConfiguration().setInt("n_second_cols", Integer.parseInt(args[4]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
