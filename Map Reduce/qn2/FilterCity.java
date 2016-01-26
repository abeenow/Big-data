
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class FilterCity {

    public static class FilterMapper
            extends Mapper<Object, Text, Text, NullWritable> {

        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] data = value.toString().split("::");
            if (data.length > 23) {
                String business_id = data[2];
                String full_address = data[3].toLowerCase();
                String type = data[22];
                if (type.equalsIgnoreCase("business")) {
                    if (full_address.contains("palo")) {
                        context.write(new Text(business_id), NullWritable.get());
                    }
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: FilterCity <input path> <output path>");
            System.exit(2);
        }
        Path input = new Path(args[0]);
        Path output = new Path(args[1]);
        FileSystem fs = FileSystem.get(conf);
        if (fs.isDirectory(output)) {
            fs.delete(output, true);
        }
        Job job = Job.getInstance(conf, "FilterCity");
        job.setJarByClass(FilterCity.class);
        job.setMapperClass(FilterMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
