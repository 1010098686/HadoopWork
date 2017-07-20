import jdk.nashorn.internal.scripts.JO;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by fk on 17-7-11.
 */
public class AppearCountMain {

    public static class AppearCountMapper extends Mapper<Object, Text, Text, IntWritable> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] names = line.split(" ");
            Set<String> nameSet = new HashSet<String>();
            for (String str : names) {
                nameSet.add(str);
            }
            List<String> nameList = new ArrayList<String>(nameSet);
            int size = nameList.size();
            for (int i = 0; i < size; ++i) {
                for (int j = 0; j < size; ++j) {
                    if (i != j) {
                        String result = "<" + nameList.get(i) + "," + nameList.get(j) + ">";
                        context.write(new Text(result), new IntWritable(1));
                    }
                }
            }

        }
    }

    public static class AppearCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable it : values) {
                sum += it.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }
}
