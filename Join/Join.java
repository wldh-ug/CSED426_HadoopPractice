import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.LinkedList;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class Join {

    // Complete the JoinMapper class.
    // Definitely, Generic type (LongWritable, Text, Text, Text) must not be
    // modified
    public static class JoinMapper extends Mapper<LongWritable, Text, Text, Text> {
        // You have to use these variables to generalize the Join.
        String[] tableNames = new String[2];
        String first_table_name = null;
        int first_table_join_index;
        String second_table_name = null;
        int second_table_join_index;

        protected void setup(Context context) throws IOException, InterruptedException {
            // Don't change setup function
            tableNames = context.getConfiguration().getStrings("table_names");
            first_table_join_index = context.getConfiguration().getInt("first_table_join_index", 0);
            second_table_join_index =
                    context.getConfiguration().getInt("second_table_join_index", 0);
            first_table_name = tableNames[0];
            second_table_name = tableNames[1];
        }

        public void map(LongWritable key, Text record, Context context)
                throws IOException, InterruptedException {
            // Implement map function

            String[] params = record.toString().split("\",\"");
            params[0] = params[0].substring(1);
            if (params.length > 1) {

                params[params.length - 1] = params[params.length - 1].substring(0,
                        params[params.length - 1].length() - 2);

            }

            if (params[0].equals(first_table_name)) {

                context.write(new Text(params[first_table_join_index]), record);

            } else if (params[0].equals(second_table_name)) {

                context.write(new Text(params[second_table_join_index]), record);

            }

        }

    }

    // Don't change (key, value) types
    public static class JoinReducer extends Reducer<Text, Text, Text, Text> {
        String[] tableNames = new String[2];
        String first_table_name = null;
        String second_table_name = null;

        protected void setup(Context context) throws IOException, InterruptedException {
            // Similar to Mapper Class
            tableNames = context.getConfiguration().getStrings("table_names");
            first_table_name = tableNames[0];
            second_table_name = tableNames[1];
        }

        public void reduce(Text order_id, Iterable<Text> records, Context context)
                throws IOException, InterruptedException {
            // Implement reduce function
            // You can see form of new (key, value) pair in sample output file on server.
            // You can use Array or List or other Data structure for 'cache'.
            // context.write(newKey, newValue)

            List<String> aList = new LinkedList<String>();
            List<String> bList = new LinkedList<String>();

            for (Text rec : records) {

                String name = rec.toString().split(",")[0];

                if (name.substring(1, name.length() - 1).equals(first_table_name)) {

                    aList.add(rec.toString());

                } else {

                    bList.add(rec.toString());

                }

            }

            for (String a : aList) {

                for (String b : bList) {

                    context.write(order_id, new Text(a + "," + b));

                }

            }

        }

    }

    public static void main(String[] args)
            throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Table Join");

        job.setJarByClass(Join.class);
        job.setMapperClass(JoinMapper.class);
        job.setReducerClass(JoinReducer.class);

        job.setInputFormatClass(TextInputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.getConfiguration().setStrings("table_names", args[2]);
        job.getConfiguration().setInt("first_table_join_index", Integer.parseInt(args[3]));
        job.getConfiguration().setInt("second_table_join_index", Integer.parseInt(args[4]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
