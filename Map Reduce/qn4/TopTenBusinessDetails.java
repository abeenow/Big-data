
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.util.GenericOptionsParser;

public class TopTenBusinessDetails {

    private static final int LIMIT = 10;
    private static Map<String, Float> map1 = new HashMap<>();
    private static Map<String, String> map2 = new HashMap<>();

    public static class ReviewMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] data = value.toString().split("::");
            if (data.length > 23) {
                String type = data[22];
                String business_id = data[2];
                if (type.equalsIgnoreCase("review")) {
                    context.write(new Text(business_id), new Text(data[20]));
                }
            }
        }
    }

    public static class BusinessMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] data = value.toString().split("::");
            if (data.length > 23) {
                String type = data[22];
                String business_id = data[2];
                if (type.equalsIgnoreCase("business")) {
                    context.write(new Text(business_id), new Text("Bus:" + data[3] + "\t" + data[10]));
                }
            }
        }
    }

    public static class ReviewReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            float sum = 0F;
            float count = 0F;
            String details = "";
            for (Text val : values) {
                String newVal = val.toString();
                if (newVal.contains("Bus:")) {
                    details = newVal.substring(4);
                } else {
                    sum += Float.parseFloat(newVal);
                    count++;
                }
            }
            float result = (sum / count);
            if (!details.equalsIgnoreCase("") && !Float.isNaN(result)) {
                if (map1.size() < LIMIT) {
                    map1.put(key.toString(), result);
                    map2.put(key.toString(), details);
                } else {
                    float min_val = Collections.min(map1.values());
                    if (min_val < result) {
                        String remKey = new String();
                        for (String itKey : map1.keySet()) {
                            if (map1.get(itKey) == min_val) {
                                remKey = itKey;
                            }
                        }
                        map1.remove(remKey);
                        map2.remove(remKey);
                        map1.put(key.toString(), result);
                        map2.put(key.toString(), details);
                    }
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(new Text("Business ID"), new Text("Full Address\tCategories\tAvg. Rating"));
            for (String key : map1.keySet()) {
                context.write(new Text(key), new Text(map2.get(key) + "\t" + map1.get(key)));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        GenericOptionsParser cmdLine = new GenericOptionsParser(conf, args);
        String[] otherArgs = cmdLine.getRemainingArgs();
        if (otherArgs.length != 1) {
            System.err.println("Usage: TopTenBusinessDetails -D business.data=<data path> -D review.data=<data path> <output path>");
            System.exit(2);
        }
        Path business_data = new Path(conf.get("business.data"));
        Path review_data = new Path(conf.get("review.data"));
        Path output = new Path(otherArgs[0]);
        FileSystem fs = FileSystem.get(conf);
        if (!fs.exists(business_data) || !fs.exists(review_data)) {
            System.err.println("Error: Missing data files\n Usage of -D business.data=<data path> -D review.data=<data path> is mandatory");
            System.exit(3);
        }
        if (fs.isDirectory(output)) {
            fs.delete(output, true);
        }

        Job job = Job.getInstance(conf, "TopTenBusinessDetails");
        job.setJarByClass(TopTenBusinessDetails.class);

        MultipleInputs.addInputPath(job, business_data, TextInputFormat.class, BusinessMapper.class);
        MultipleInputs.addInputPath(job, review_data, TextInputFormat.class, ReviewMapper.class);

        job.setReducerClass(ReviewReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, output);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

