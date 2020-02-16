package king.Utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Writable;

public class ListWritable<T extends Writable> implements Writable {
	private List<T> list;
	private Class<T> clazz;

	public ListWritable(Class<T> clazz) {
	       this.clazz = clazz;
	       list = new ArrayList<T>();
	    }
	
	public void add(T element){
		list.add(element);
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
	    out.writeUTF(clazz.getName());
	    out.writeInt(list.size());
	    for (T element : list) {
	        element.write(out);
	    }
	 }

	 @SuppressWarnings("unchecked")
	@Override
	 public void readFields(DataInput in) throws IOException{
	 try {
		clazz = (Class<T>) Class.forName(in.readUTF());
	} catch (ClassNotFoundException e1) {
		// TODO Auto-generated catch block
		e1.printStackTrace();
	}
	 int count = in.readInt();
	 this.list = new ArrayList<T>();
	 for (int i = 0; i < count; i++) {
	    try {
	        T obj = clazz.newInstance();
	        obj.readFields(in);
	        list.add(obj);
	    } catch (InstantiationException e) {
	        e.printStackTrace();
	    } catch (IllegalAccessException e) {
	        e.printStackTrace();
	    }
	  }
	}

}
