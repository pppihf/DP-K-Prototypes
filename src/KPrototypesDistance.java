import java.util.List;

import king.Utils.Distance;

public class KPrototypesDistance<T extends Number> implements Distance<T>{
	public static final double lambda = 0.12;
	@Override
	public double getDistance(List<T> a, List<T> b) throws Exception {
		// TODO Auto-generated method stub
		if(a.size() != b.size())
			throw new Exception("size not compatible!");
		else{
			double sum1 = 0.0;
			int sum2 = 0;
			for(int i = 0;i < KmeansReduce.dimension1;i++){
				sum1 += Math.pow(a.get(i).doubleValue() - b.get(i).doubleValue(), 2);      //计算平方欧式距离
			}
			for(int i = KmeansReduce.dimension1; i < KmeansReduce.dimension1 +KmeansReduce.dimension2; i++){
				if(!a.get(i).equals(b.get(i))){        //计算简单匹配距离
					sum2 = sum2 + 1;
				}
			}
			double sum = Math.sqrt(sum1) + lambda*sum2;
			return sum;
		}
	}
}
