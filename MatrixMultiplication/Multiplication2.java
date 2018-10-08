import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class Multiplication2 {
	// Complete the Matrix2_Mapper class.
	// Definitely, Generic type (Text, Text, Text, Text) must not be modified

	// Optional, you can use both 'setup' and 'cleanup' function, or either of them, or none of
	// them.
	// Optional, you can add and use new methods in this class

	public static class Matrix2_Mapper extends Mapper<Text, Text, Text, Text> {
		// Optional, Using, Adding, Modifying and Deleting variable is up to you
		int result_rows = 0;
		int result_columns = 0;
		int n_first_cols = 0;
		int n_second_cols = 0;
		int[][] a, b;
		boolean preCalculated = false;

		// Optional, Utilizing 'setup' function or not is up to you
		protected void setup(Context context) throws IOException, InterruptedException {

			result_rows = context.getConfiguration().getInt("n_first_rows", 0);
			n_first_cols = context.getConfiguration().getInt("n_first_cols", 0);
			n_second_cols = context.getConfiguration().getInt("n_second_cols", 0);
			result_columns = context.getConfiguration().getInt("n_third_cols", 0);

			a = new int[result_rows][n_first_cols];
			b = new int[n_first_cols][n_second_cols];

		}

		// Definitely, parameter type and name (Text matrix, Text entry, Context context) must not
		// be modified
		public void map(Text matrix, Text entry, Context context)
				throws IOException, InterruptedException {
			// Implement map function.

			String[] cell = entry.toString().split(",");

			// NOTE: "c" becomes after "a", "b"
			// NOTE: names of matrices are fixed
			if (matrix.toString().equals("c")) {

				if (!preCalculated) {

					preCalculated = true;

					for (int i = 0; i < result_rows; i++) {

						for (int j = 0; j < n_second_cols; j++) {

							int abCell = 0;

							for (int m = 0; m < n_first_cols; m++) {

								abCell += a[i][m] * b[m][j];

							}

							for (int k = 0; k < result_columns; k++) {

								context.write(new Text(i + "," + k), new Text(j + "," + abCell));

							}

						}

					}

				}

				for (int k = 0; k < result_rows; k++) {

					context.write(new Text(k + "," + cell[1]), new Text(cell[0] + "," + cell[2]));

				}

			} else {

				if (matrix.toString().equals("a")) {

					a[Integer.parseInt(cell[0])][Integer.parseInt(cell[1])] =
							Integer.parseInt(cell[2]);

				} else {

					b[Integer.parseInt(cell[0])][Integer.parseInt(cell[1])] =
							Integer.parseInt(cell[2]);

				}

			}

		}

	}


	// Complete the Matrix2_Reducer class.
	// Definitely, Generic type (Text, Text, Text, Text) must not be modified
	// Definitely, Output format and values must be the same as given sample output

	// Optional, you can use both 'setup' and 'cleanup' function, or either of them, or none of
	// them.
	// Optional, you can add and use new methods in this class
	public static class Matrix2_Reducer extends Reducer<Text, Text, Text, Text> {
		// Optional, Using, Adding, Modifying and Deleting variable is up to you
		int n_second_cols = 0;

		// Optional, Utilizing 'setup' function or not is up to you
		protected void setup(Context context) throws IOException, InterruptedException {
			n_second_cols = context.getConfiguration().getInt("n_second_cols", 0);
		}

		// Definitely, parameters type (Text, Iterable<Text>, Context) must not be modified
		// Optional, parameters name (key, values, context) can be modified
		public void reduce(Text entry, Iterable<Text> entryComponents, Context context)
				throws IOException, InterruptedException {
			// Implement reduce function.

			int result = 0;
			int[] partialResult = new int[n_second_cols];
			boolean[] partialAssigned = new boolean[n_second_cols];
			boolean[] partialCalculated = new boolean[n_second_cols];

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

			for (int i = 0; i < n_second_cols; i++) {

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
		Job job = Job.getInstance(conf, "Matrix Multiplication");

		job.setJarByClass(Multiplication2.class);
		job.setMapperClass(Matrix2_Mapper.class);
		job.setReducerClass(Matrix2_Reducer.class);

		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.getConfiguration().setInt("n_first_rows", Integer.parseInt(args[2]));
		job.getConfiguration().setInt("n_first_cols", Integer.parseInt(args[3]));
		job.getConfiguration().setInt("n_second_cols", Integer.parseInt(args[4]));
		job.getConfiguration().setInt("n_third_cols", Integer.parseInt(args[5]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
