
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.util.GenericOptionsParser;

public class UserDetails {

    private static List<String> list = new ArrayList();

    public static class UserReviewMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void setup(Context context) throws IOException, InterruptedException, FileNotFoundException {
            super.setup(context);
            Configuration conf = context.getConfiguration();
            Path file = new Path(conf.get("business.data"));
            FileSystem fs = FileSystem.get(conf);
            BufferedReader brReader = new BufferedReader(new InputStreamReader(fs.open(file)));
            String line;
            while ((line = brReader.readLine()) != null) {
                String[] data = line.split("::");
                if (data.length > 23) {
                    String type = data[22];
                    String business_id = data[2];
                    if (type.equalsIgnoreCase("business")) {
                        String full_address = data[3].toLowerCase();
                        if (full_address.contains("stanford")) {
                            list.add(business_id);
                        }
                    }
                }
            }
            brReader.close();
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] data = value.toString().split("::");
            if (data.length > 23) {
                String type = data[22];
                String business_id = data[2];
                if (type.equalsIgnoreCase("review")) {
                    if (list.contains(business_id)) {
                        String user_id = data[8];
                        String review_text = data[1];
                        context.write(new Text(user_id), new Text(review_text));
                    }
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        GenericOptionsParser cmdLine = new GenericOptionsParser(conf, args);
        String[] otherArgs = cmdLine.getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: UserDetails -D business.data=<data path> <input path> <output path>");
            System.exit(2);
        }

        Path business_data = new Path(conf.get("business.data"));
        Path input = new Path(otherArgs[0]);
        Path output = new Path(otherArgs[1]);
        FileSystem fs = FileSystem.get(conf);

        if (!fs.exists(business_data)) {
            System.err.println("Error: Missing data files\n Usage of -D business.data=<data path> is mandatory");
            System.exit(3);
        }

        if (fs.isDirectory(output)) {
            fs.delete(output, true);
        }

        Job job = Job.getInstance(conf, "UserDetails");
        job.setJarByClass(UserDetails.class);
        job.addCacheFile(new URI(business_data.toString()));

        job.setMapperClass(UserReviewMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
