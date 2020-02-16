package king.Utils;

import java.util.List;

public class EuclideanDistance<T extends Number> implements Distance<T> {

	@Override
	public double getDistance(List<T> a, List<T> b) throws Exception {
		// TODO Auto-generated method stub
		if(a.size() != b.size())
			throw new Exception("size not compatible!");
		else{
			double sum = 0.0;
			for(int i = 0;i < a.size();i++){
				sum += Math.pow(a.get(i).doubleValue() - b.get(i).doubleValue(), 2);
			}
			sum = Math.sqrt(sum);
			return sum;
		}
	}

}
