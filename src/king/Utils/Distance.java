package king.Utils;

import java.util.List;

public interface Distance<T> {
	double getDistance(List<T> a,List<T> b) throws Exception;
}
