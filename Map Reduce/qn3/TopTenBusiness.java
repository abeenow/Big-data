
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class TopTenBusiness {

    private static final int LIMIT = 10;

    public static class AvgMapper
            extends Mapper<Object, Text, Text, FloatWritable> {

        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] data = value.toString().split("::");
            if (data.length > 23) {
                String type = data[22];
                if (type.equalsIgnoreCase("review")) {
                    String business_id = data[2];
                    Float rating = new Float(data[20]);
                    context.write(new Text(business_id), new FloatWritable(rating));
                }
            }
        }
    }

    public static class AvgReducer
            extends Reducer<Text, FloatWritable, Text, NullWritable> {

        private Map<String, Float> map = new HashMap<>();

        @Override
        public void reduce(Text key, Iterable<FloatWritable> values,
                Context context
        ) throws IOException, InterruptedException {
            float sum = 0F;
            float count = 0F;
            for (FloatWritable val : values) {
                sum += val.get();
                count++;
            }
            float result = (sum / count);
            if (map.size() < LIMIT) {
                map.put(key.toString(), result);
            } else {
                float min_val = Collections.min(map.values());
                if (min_val < result) {
                    String remKey = new String();
                    for (String itKey : map.keySet()) {
                        if (map.get(itKey) == min_val) {
                            remKey = itKey;
                        }
                    }
                    map.remove(remKey);
                    map.put(key.toString(), result);
                }
            }
        }

        @Override
        protected void cleanup(Context context)
                throws IOException,
                InterruptedException {
            context.write(new Text("Business ID"), NullWritable.get());
            for (String key : map.keySet()) {
                context.write(new Text(key), NullWritable.get());
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: TopTenBusiness <input path> <output path>");
            System.exit(2);
        }
        Path input = new Path(args[0]);
        Path output = new Path(args[1]);
        FileSystem fs = FileSystem.get(conf);
        if (fs.isDirectory(output)) {
            fs.delete(output, true);
        }

        Job job = Job.getInstance(conf, "TopTenBusiness");
        job.setJarByClass(TopTenBusiness.class);
        job.setMapperClass(AvgMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FloatWritable.class);
        job.setReducerClass(AvgReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
