import java.util.ArrayList;
import java.util.List;
/**
Authors:
 Harikrishna Bathala (1001415489)
 Rohit Katta (1001512896)
 *
 */
// Lock Class for which a hashmap with key Item_Name,and values are Lock_Mode, list Locking_Transactions, list Waiting_Transactions
public class Lock {
	public String Item_Name;
	public String Lock_Mode="UL";
	public List<String> Locking_Transactions=new ArrayList();
	public List<String> Waiting_Transactions=new ArrayList();


}
