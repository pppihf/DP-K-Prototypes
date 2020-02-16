import org.apache.hadoop.fs.Path;

public class CommonArgument {
	public static final int K=2;//聚类的类别数
	public static final String inputlocation="hdfs://localhost:9000/user/hadoop/data.txt";
	public static final String outputlocation="hdfs://localhost:9000/user/hadoop/results";
	public static final String canopylocation="hdfs://localhost:9000/user/hadoop/canopy";
	public static final String center_inputlocation="hdfs://localhost:9000/user/hadoop/canopy/part-r-00000";
	public static final String center_outputlocation="hdfs://localhost:9000/user/hadoop/output_centers";
	public static final String new_center_outputlocation="hdfs://localhost:9000/user/hadoop/output_centers/part-r-00000";
	public static final int REPEAT=2;
	public static final double threshold=(double)0.0001;
	public static final double epsilon=1.5;
	public static Path inputpath=new Path(inputlocation);
	public static Path outputpath=new Path(outputlocation);
	public static Path canopypath=new Path(canopylocation);
	public static Path center_inputpath=new Path(center_inputlocation);
	public static Path center_outputpath=new Path(center_outputlocation);
}
