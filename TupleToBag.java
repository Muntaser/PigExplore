// Java code for TupleToBag.java

package myudfs;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.data.DataType;
import org.apache.pig.data.DefaultTuple;

public class TupleToBag extends EvalFunc<DataBag>
{
    public DataBag exec(Tuple input) throws IOException {
        if (null == input || input.size() == 0) {
            return null;
        }

        String line = (String) input.get(0);
        try {

            TupleFactory tf = TupleFactory.getInstance();
            BagFactory bf = BagFactory.getInstance();
	    DataBag outputBag = bf.newDefaultBag();


			
// process the line using split string
			String[] l = line.split(",");
			String[] bills = l[1].split(" ");

//Generate tuple
		for(int i=0; i < bills.length; i++){
			Tuple t = tf.newTuple();
                         t.append(l[0]);
			 t.append(Integer.parseInt(bills[i]));
			 outputBag.add(t);
		}
						
            return outputBag;
			
        } catch (Exception e) {
            return null;
        }
    }

  
public Schema outputSchema(Schema input) {
    
try{
      Schema tupleSchema = new Schema();
      tupleSchema.add(new Schema.FieldSchema("tellerName", DataType.CHARARRAY));
      tupleSchema.add(new Schema.FieldSchema("bill", DataType.INTEGER));

      return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input), 		   tupleSchema, DataType.BAG));
    } catch (Exception e) {           
	return null;        
      }
  }


}



