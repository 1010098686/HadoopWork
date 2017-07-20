/**
 * Created by caiwenpu on 2017/7/11.
 */
/**
 * Created by caiwenpu on 2017/5/2.
 */
/**
 * Created by cai_hadoop on 4/21/17.
 */
import java.io.*;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.LineReader;
import org.apache.commons.lang.StringUtils;

class Utility{
	public static void Read_peoplename_label(HashMap<String , Integer> Peoplename_map,Configuration jobconf) throws IOException {
		String uri = jobconf.get("peoplename_label_path");
		FileSystem fs = FileSystem.get(jobconf);
		FileStatus[] status = fs.listStatus(new Path(uri));
		for (FileStatus file : status) {
			if (!file.getPath().getName().startsWith("part-")) {
				continue;
			}
			FSDataInputStream in = fs.open(file.getPath());
			BufferedReader bf = new BufferedReader(new InputStreamReader(in)); //BufferedReader 包装各种 Reader
			String line;
			while((line = bf.readLine()) != null)
				Peoplename_map.put(line.split("\t")[0] , Integer.parseInt(line.split("\t")[1]));
			in.close();
		}
	}

	public static void Read_Degree(HashMap<String , Integer> PeopleDegree_map,Configuration jobconf) throws IOException {
		String uri = jobconf.get("peopleDegree_path");
		FileSystem fs = FileSystem.get(jobconf);
		FileStatus[] status = fs.listStatus(new Path(uri));
		for (FileStatus file : status) {
			if (!file.getPath().getName().startsWith("part-")) {
				continue;
			}
			FSDataInputStream in = fs.open(file.getPath());
			BufferedReader bf = new BufferedReader(new InputStreamReader(in)); //BufferedReader 包装各种 Reader
			String line;
			int index = 0;
			while ((line = bf.readLine()) != null) {
				String []strs = line.split("\t");
				PeopleDegree_map.put(strs[0], Integer.parseInt(strs[1]));
				if(index > 20) break;
				index++;
			}
			in.close();
		}
	}

	public static void Read_peoplename_list(HashMap<String , Integer> Peoplename_map,Configuration jobconf) throws IOException {
		String uri = jobconf.get("peoplename_path");
		FileSystem fs = FileSystem.get(jobconf);
		FSDataInputStream in = fs.open(new Path(uri));
		BufferedReader bf = new BufferedReader(new InputStreamReader(in));
		String line;
		int index = 0;
		while ((line = bf.readLine()) != null){
			Peoplename_map.put(line , index);
			index++;
		}
		in.close();
	}

	public static boolean deleteDir(String dir) throws IOException {
		if (StringUtils.isBlank(dir)) {
			return false;
		}
		dir = dir;
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(dir), true);
		fs.close();
		return true;
	}

	public static String [] Read_Pathconf(String path_conf) throws IOException {
		File file = new File(path_conf);
		BufferedReader br = new BufferedReader(new FileReader(file));
		String []Paths_Dir = br.readLine().split(" ");
		String []Paths = new String[Paths_Dir.length+1];
		Paths[0] = Paths_Dir[0];
		Paths[1] = Paths_Dir[1];
		Paths[2] = Paths_Dir[2] + "/Label_res";
		Paths[3] = Paths_Dir[2] + "/Stat_Degree";
		return Paths;
		//System.out.println(Paths[2]+" " + Paths[3]);
	}
}
class Init_Label {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, Text> {

        //private final static IntWritable one = new IntWritable(1);
        HashMap<String , Integer> Peoplename_map = new HashMap<String , Integer>();


        public void setup(Context context) throws IOException {
            Configuration jobconf  = context.getConfiguration();
            Utility.Read_peoplename_list(Peoplename_map,jobconf);
        }
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
//            FileSplit fileSplit = (FileSplit)context.getInputSplit();
//            String fileName = fileSplit.getPath().getName();
	        String node = value.toString().split("\t")[0];
	        String list = value.toString().split("\t")[1];
            context.write(new Text(node),new Text(String.valueOf(Peoplename_map.get(node)))); //<key,value> = <name,label>
            }
        }

    public static class IntSumReducer
            extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            for(Text v : values)
                context.write(key, v);
        }
    }

}

class Stat_Degree {

	public static class Degree_Mapper
			extends Mapper<Object, Text, IntWritable, Text> {

		public void map(Object key, Text value, Context context
		) throws IOException, InterruptedException {
			String node = value.toString().split("\t")[0];
			String list = value.toString().split("\t")[1];
			String []neighbours = list.substring(1,list.length()-1).split("\\|");
			context.write(new IntWritable(neighbours.length),new Text(node)); //<key,value> = <degree,node>
		}
	}

	public static class Degree_Reducer
			extends Reducer<IntWritable, Text, Text, IntWritable> {
		public void reduce(IntWritable key, Iterable<Text> values,
		                   Context context
		) throws IOException, InterruptedException {
			for(Text v : values) {
					context.write(v, key);
			}
		}
	}

	public static class IntComparator extends IntWritable.Comparator {
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			return -super.compare(b1, s1, l1, b2, s2, l2);
		}
	}


}


//初始化标签为雪花形状
class Preprop_Label {

	public static class Preprop_Mapper
			extends Mapper<Object, Text, Text, IntWritable> {

		HashMap<String , Integer> PeopleDegree_map = new HashMap<String , Integer>();
		HashMap<String , Integer> Peoplename_map = new HashMap<String , Integer>();
		public void setup(Context context) throws IOException {
			Configuration jobconf  = context.getConfiguration();
			Utility.Read_Degree(PeopleDegree_map,jobconf);
			Utility.Read_peoplename_label(Peoplename_map,jobconf);
		}

		public void map(Object key, Text value, Context context
		) throws IOException, InterruptedException {
			String node = value.toString().split("\t")[0];
			String list = value.toString().split("\t")[1];
			String []neighbours = list.substring(1,list.length()-1).split("\\|");
			int Max_degree = 0;
			String Max_degree_ne = "";
			for (String neighbour : neighbours) {
				String[] name_weight = neighbour.split(",");
				if (PeopleDegree_map.containsKey(name_weight[0])) {
					int cur_degree = PeopleDegree_map.get(name_weight[0]);
					if (cur_degree > Max_degree) {
						Max_degree = cur_degree;
						Max_degree_ne = name_weight[0];
					}
				}
			}
			if(PeopleDegree_map.containsKey(node) || Max_degree_ne.equals("")) Max_degree_ne = node;
			context.write(new Text(node),new IntWritable(Peoplename_map.get(Max_degree_ne))); //<key,value> = <degree,node>
		}
	}

	public static class Preprop_Reducer
			extends Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values,
		                   Context context
		) throws IOException, InterruptedException {
			for(IntWritable v : values) {
					context.write(key, v);
			}
		}
	}

}

class Prop_label{
	public static class Prop_Mapper
			extends Mapper<Object, Text, Text, IntWritable>
	{
		HashMap<String , Integer> Peoplename_label = new HashMap<String , Integer>();
		public void setup(Context context) throws IOException {
			Configuration jobconf  = context.getConfiguration();
			Utility.Read_peoplename_label(Peoplename_label, jobconf);
		}
		public void map(Object key, Text value, Context context
		) throws IOException, InterruptedException {
			//统计每种label的权重
			String node = value.toString().split("\t")[0];
			String list = value.toString().split("\t")[1];
			String []neighbours = list.substring(1,list.length()-1).split("\\|");
			HashMap<Integer , Double> neighbours_map = new HashMap<Integer, Double>();
			for (String neighbour : neighbours) {
				String[] name_weight = neighbour.split(",");
				int t_label = Peoplename_label.get(name_weight[0]);
				if (!neighbours_map.containsKey(t_label))
					neighbours_map.put(t_label,0.0);
				neighbours_map.put(t_label, neighbours_map.get(t_label) + Double.parseDouble(name_weight[1]));
			}

			//计算权重最大的label
			ArrayList <Integer> mymax = new ArrayList<Integer>();
			double mm = 0.0;
			for (int k : neighbours_map.keySet()) {
				//System.out.print(k+":"+neighbours_map.get(k)+" ");
				mm = Math.max(neighbours_map.get(k),mm);
			}
			//System.out.println("");
			for(int k : neighbours_map.keySet())
			{
				if(Math.abs(neighbours_map.get(k)-mm) < 0.0001) {
					//System.out.print(k+":"+neighbours_map.get(k)+" ");
					mymax.add(k);
				}
			}
			//System.out.println("");
			//System.out.println("");
			Random rand = new Random();
			int randNum = rand.nextInt(mymax.size());

			int new_label = mymax.get(randNum);
			int old_label = Peoplename_label.get(node);

			if(new_label == old_label)
				context.write(new Text(node + "#0"),new IntWritable(new_label));
			else
				context.write(new Text(node +"#1"),new IntWritable(new_label));
		}
	}

	public static class Prop_Reducer
			extends Reducer<Text, IntWritable, Text, IntWritable> {
		int label_change_num = 0;
		public void reduce(Text key, Iterable<IntWritable> values,
		                   Context context
		) throws IOException, InterruptedException {
			label_change_num += Integer.parseInt(key.toString().split("#")[1]);
			for(IntWritable v : values) {
				context.write(new Text(key.toString().split("#")[0]), v);
			}
		}
		public void cleanup(Context context)
		{
			//System.out.println(label_change_num);
			Configuration jobconf = context.getConfiguration();
			int raw = jobconf.getInt("label_changes",0);
			jobconf.setInt("label_changes",raw+label_change_num);
			System.out.println(jobconf.getInt("label_changes",0));
		}
	}


}
