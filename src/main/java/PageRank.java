/**
 * Created by rajesh on 17-7-11.
 */
import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PageRank {

    private static final double damping = 0.85;

    public static class MyMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] tmp = line.split("\t");
            String pk = tmp[0];
            double pr = Double.parseDouble(tmp[1]);

            if(tmp.length > 2) {
                String[] link = tmp[2].split("\\|");
                link[0] = link[0].substring(1);
                link[link.length-1] = link[link.length-1].substring(0,link[link.length-1].length()-1);
                for(String l:link) {
                    String linkPage = l.split(",")[0];
                    String weight = l.split(",")[1];
                    double prValue = pr * Double.parseDouble(weight);
                    context.write(new Text(linkPage), new Text(String.valueOf(prValue)));
                }
                context.write(new Text(pk), new Text("|" + tmp[2]));
            }
        }
    }

    public static class MyReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String link = "";
            double pr = 0;
            for(Text v:values) {
                String tmp = v.toString();
                if(tmp.startsWith("|")) {
                    link = tmp.substring(tmp.indexOf("|")+1);
                }
                else {
                    pr += Double.parseDouble(v.toString());
                }
            }

            pr = 1 - damping + damping * pr;
            context.write(key, new Text(String.valueOf(pr) + "\t" + link));
        }
    }

}
