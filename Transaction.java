import java.util.ArrayList;
import java.util.List;

/**
 Authors:
 Harikrishna Bathala (1001415489)
 Rohit Katta (1001512896)
 *
 */
// Trasaction Class for which a hashmap with key TrasacionID,and values are Transaction_State, Timestamp, list Items_Held, list Waiting_Operation
public class Transaction {
		public String TransactionID;
		public String Transaction_State;
		public Integer Timestamp;
		public List<String> Items_Held=new ArrayList();;
		public List<String> Waiting_Operation=new ArrayList();

}
