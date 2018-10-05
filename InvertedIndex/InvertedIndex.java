import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InvertedIndex {

    // NOTE: <KEYIN,VALUEIN,KEYOUT,VALUEOUT>
    public static class GrindingMapper extends Mapper<Object, Text, Text, Text> {

        private Text word = new Text(), source = new Text();

        public void setup(Context context) {

            source.set(((FileSplit) context.getInputSplit()).getPath().getName());

        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {

                word.set(itr.nextToken().toLowerCase().replaceAll("\\W+", ""));
                context.write(word, source);

            }

        }

    }

    // NOTE: <KEYIN,VALUEIN,KEYOUT,VALUEOUT>
    public static class GatheringReducer extends Reducer<Text, Text, Text, Text> {

        private Text sourceLong = new Text();

        public void reduce(Text word, Iterable<Text> sources, Context context)
                throws IOException, InterruptedException {

            String sourceConcatten = new String();

            for (Text source : sources) {

                sourceConcatten += source.toString() + ",";

            }

            sourceLong.set(sourceConcatten.substring(0, sourceConcatten.length() - 1));
            context.write(word, sourceLong);

        }

    }

    public static void main(String[] args) throws Exception {

        Configuration config = new Configuration();
        Job job = Job.getInstance(config, "Inverted Index");

        job.setJarByClass(InvertedIndex.class);
        job.setMapperClass(GrindingMapper.class);
        job.setReducerClass(GatheringReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}
