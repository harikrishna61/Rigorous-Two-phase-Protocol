import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
/**
 Authors:
 Harikrishna Bathala (1001415489)
 Rohit Katta (1001512896)
 *
 */
//Main class to run 2phase locking with wait and die dead lock prevention protocol
public class Two_phase_locking {
	// Hash map's for Transaction table and Lock Table and Timestamp is declared globally and updated by 1 unit for each transaction in transaction table
	public static HashMap<String, Transaction> TranTable = new HashMap<String, Transaction>();
	public static HashMap<String, Lock> LockTable = new HashMap<String, Lock>();
	public static Integer Timestamp = 0;
	// Reading the input file line line and parsing the line using parseInput method
	public static void main(String[] args) {
		String filePath = "";
		if(args.length > 0){
			filePath = args[0];
		} else {
			filePath = "D:\\inp.txt";
		}
		try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
			String line;
			while ((line = br.readLine()) != null) {
				// process the line.
				line = line.replace(" ", "");
				line = line.trim();
				parseInput(line);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
// parseInput method simulates the 2 phase locking protocol by reading each input and updates transaction and Lock table accordingly.
	public static void parseInput(String line) {
		System.out.println();
		System.out.println(line.substring(0, line.length() - 1));
		String firstChar = line.substring(0, 1);
		switch (firstChar) {
			// reading 'b' in input line creates a new row in Transaction table with state 'active' ,id and timestamp based on input line.
			case "b": {
				System.out.println("Transaction has begun");
				Transaction tr = new Transaction();
				tr.TransactionID = line.substring(1, line.length() - 1);
				Timestamp = Timestamp + 1;
				tr.Timestamp = Timestamp;
				tr.Transaction_State = "Active";
				TranTable.put(tr.TransactionID, tr);
				System.out.println("TranasctionID - "+tr.TransactionID+" Timestamp - " + Timestamp + " State - "+"Active");
				break;
			}
			// reading 'r' either provides provides read for particular item or aborts or blocks the Transaction based on wait and die.
			//Adding/Making changes to Lock table.
			//Adding/Making changes to Transaction table.
			case "r": {
				try {
					String itemName = line.substring(line.indexOf('(') + 1, line.indexOf(')'));
					String tranId = line.substring(1, line.indexOf('('));
//					String itemName = line.substring(line.indexOf('(') + 1, line.indexOf(')'));
//					String tranId = line.substring(1, line.indexOf(' '));
					Transaction tr = TranTable.get(tranId);
					String get_tran_state = TranTable.get(tranId).Transaction_State;
					//perform only for the Transaction with active state .
					if (get_tran_state.equals("Active")) {

						// Append current Transaction ID in
						// item.Locking_Transactions
						try {
							Lock lck = LockTable.get(itemName);
							String get_lock_mode;
							get_lock_mode = LockTable.get(itemName).Lock_Mode;
							//Removing Transaction from waiting list
							if(lck.Waiting_Transactions.contains(tranId))
							{
								lck.Waiting_Transactions.remove(tranId);
							}
							if (get_lock_mode == "RL"  ) {
								try {
									List lock_transactions;
									lock_transactions = LockTable.get(itemName).Locking_Transactions;
									lock_transactions.add(tranId);
									lck.Locking_Transactions = lock_transactions;
									LockTable.put(itemName, lck);
									System.out.println("Read Lock acquired for " + itemName);
									System.out.println("New lock table for " + itemName + " is "+ LockTable.get(itemName).Locking_Transactions);

								} catch (Exception e1) {
									List lock_trans = new ArrayList();
									lock_trans.add(tranId);
									lck.Locking_Transactions = lock_trans;
									LockTable.put(itemName, lck);
									System.out.println("Read Lock acquired for " + itemName);
									System.out.println("New lock table for " + itemName + " is "+ LockTable.get(itemName).Locking_Transactions);

								}
								// Update Items_held in Transaction_Table where
								// Transaction_Id = tid
								try {
									List items_held;
									items_held = TranTable.get(tranId).Items_Held;
									items_held.add(itemName);
									tr.Items_Held = items_held;
									TranTable.put(TranTable.get(tranId).TransactionID, tr);
									System.out.println("Items held by Transaction " + tranId + " are "+ TranTable.get(tranId).Items_Held);
								} catch (Exception e3) {
									List add_items = new ArrayList();
									add_items.add(itemName);
									tr.Items_Held = add_items;
									TranTable.put(TranTable.get(tranId).TransactionID, tr);
									System.out.println("Items held by Transaction " + tranId + " are "+ TranTable.get(tranId).Items_Held);

								}
							}
							// Lock mode is "WL"
							else if ( get_lock_mode == "WL"){
								// Wait-and-Die
								System.out.println("Write Read Conflict! between Transaction " + lck.Locking_Transactions + " and Transaction " + tr.TransactionID);
								Wait_and_Die(line, itemName, tranId);
							}
							else
							{
								// Append current Transaction ID in
								// item.Locking_Transactions.
								Lock lck1 = LockTable.get(itemName);
								lck1.Lock_Mode = "RL";
								List add_trans = new ArrayList();
								add_trans.add(tranId);
								lck1.Locking_Transactions = add_trans;
								LockTable.put(itemName, lck1);
								System.out.println("Read Lock acquired for " + itemName);
								System.out.println("New lock table for " + itemName + " is "+ LockTable.get(itemName).Locking_Transactions);

								// Update Items_held in Transaction_Table where
								// Transaction_Id = tid
								try {
									List items_held;
									items_held = TranTable.get(tranId).Items_Held;
									items_held.add(itemName);
									tr.Items_Held = items_held;
									TranTable.put(TranTable.get(tranId).TransactionID, tr);
									System.out.println("Items held by Transaction " + tranId + " are "+ TranTable.get(tranId).Items_Held);

								} catch (Exception e3) {
									List add_items = new ArrayList();
									add_items.add(itemName);
									tr.Items_Held = add_items;
									TranTable.put(TranTable.get(tranId).TransactionID, tr);
									System.out.println("Items held by Transaction " + tranId + " are "+ TranTable.get(tranId).Items_Held);

								}
							}
						}
						// Append current Transaction ID in
						// item.Locking_Transactions.
						catch (Exception e2) {
							Lock lck = new Lock();
							lck.Item_Name = itemName;
							lck.Lock_Mode = "RL";
							List add_trans = new ArrayList();
							add_trans.add(tranId);
							lck.Locking_Transactions = add_trans;
							LockTable.put(itemName, lck);
							System.out.println("Read Lock acquired for " + itemName);
							System.out.println("New lock table for " + itemName + " is "+ LockTable.get(itemName).Locking_Transactions);
							// Update Items_held in Transaction_Table where
							// Transaction_Id = tid
							try {
								List items_held;
								items_held = TranTable.get(tranId).Items_Held;
								items_held.add(itemName);
								tr.Items_Held = items_held;
								TranTable.put(TranTable.get(tranId).TransactionID, tr);
								System.out.println("Items held by Transaction " + tranId + " are "+ TranTable.get(tranId).Items_Held);

							} catch (Exception e3) {
								List add_items = new ArrayList();
								add_items.add(itemName);
								tr.Items_Held = add_items;
								TranTable.put(TranTable.get(tranId).TransactionID, tr);
								System.out.println("Items held by Transaction " + tranId + " are "+ TranTable.get(tranId).Items_Held);
							}
						}

					}
					// if transaction state is blocked then the current operation is added to waiting operations in Transaction table of that transaction.
					else if (get_tran_state == "blocked") {
						try {
							List waiting_transactions;
							waiting_transactions = TranTable.get(tranId).Waiting_Operation;
							waiting_transactions.add(line);
							tr.Waiting_Operation = waiting_transactions;
							TranTable.put(tranId, tr);
							System.out.println("Current transaction is blocked and operation " + line
									+ " is added to waiting Operations");
							System.out.println("Waiting Operations of Transaction "+tranId+" are "+ waiting_transactions);
						} catch (Exception e) {
							tr.Waiting_Operation.add(line);
							TranTable.put(tranId, tr);
							System.out.println("Current transaction is blocked and operation " + line
									+ " is added to waiting Operations");
							System.out.println("Waiting operations of Transaction"+tranId+" are"+tr.Waiting_Operation);
						}
					}
					// if transaction state  is aborted then ignoring the current operation.
					else {
						System.out.println("Transaction " + tranId + " is already aborted");
					}
				} catch (Exception e4) {
					//e4.printStackTrace();
					System.out.println("Transaction is not active");
				}
				break;
			}
			// reading 'w' in input means either acquiring write lock or aborting or waiting the current Transaction.
			case "w": {
				String itemName = line.substring(line.indexOf('(') + 1, line.indexOf(')'));
				String tranId = line.substring(1, line.indexOf('('));
				Transaction tr = (Transaction) TranTable.get(tranId);
				Lock item;
				//getting the object of the paricular item if exists or creating it.
				if (LockTable.containsKey(itemName)) {
					item = LockTable.get(itemName);
				} else {
					item = new Lock();
					item.Item_Name = itemName;
					item.Lock_Mode = "UL";
					LockTable.put(itemName, item);
				}
				//performs only for active Transactions.
				if (tr.Transaction_State == "Active") {
					//If the item already holds read lock then check if the current Transaction holds that lock, if true then upgrading the lock to write lock
					//else applying wait and die logic to determines the current Transaction waits or aborts.
					if (item.Lock_Mode.equals("RL")) {
						// Checking if only the current transaction holds RL
						if (item.Locking_Transactions.size() == 1 && item.Locking_Transactions.contains(tranId)) {
							item.Lock_Mode = "WL";
							System.out.printf("Transaction %s upgraded item %s to WL \n", tranId, itemName);
							System.out.println("New lock table for " + itemName + " is "+ LockTable.get(itemName).Locking_Transactions);
							//Removing current transaction from waiting transactions, as it started execution
							try{
								item.Waiting_Transactions.remove(tranId);
							}
							catch (Exception e2){
							}
						}
						// Current tran hold RL along with other transactions
						else if (item.Locking_Transactions.size() > 1 && item.Locking_Transactions.contains(tranId)) {
							System.out.println("Read Write Conflict! between Transaction " + item.Locking_Transactions + " and Transaction " + tr.TransactionID);
							Wait_and_Die(line, itemName, tranId);
						}
						// If some other transaction holds RL
						else if (!item.Locking_Transactions.contains(tranId)) {
							if (item.Locking_Transactions.isEmpty()) {
								item.Lock_Mode = "WL";
								item.Locking_Transactions.add(tranId);
								System.out.printf("Transaction %s upgraded item %s to WL \n", tranId, itemName);
								System.out.println("New lock table for " + itemName + "is "+ LockTable.get(itemName).Locking_Transactions);
								//Removing current transaction from waiting transactions, as it started execution
								try{
									item.Waiting_Transactions.remove(tranId);
								}
								catch (Exception e2){
								}
							} else {
								Wait_and_Die(line, itemName, tranId);
							}
						}
					} else if (item.Lock_Mode == "WL") {
						// Applying Die-Wait logic here
						System.out.println("Write Write Conflict between Transaction " + item.Locking_Transactions.get(0) + " and Transaction " + tr.TransactionID);
						Wait_and_Die(line, itemName, tranId);
					} else if (item.Lock_Mode == "UL") {
						System.out.printf("Transaction %s acquired WL on item %s\n", tranId, itemName);
						System.out.println("New lock table for " + itemName + " is "+ LockTable.get(itemName).Locking_Transactions);
						List<String> tmpList = item.Locking_Transactions;
						tmpList.add(tranId);
						item.Locking_Transactions = tmpList;
						item.Lock_Mode = "WL";
						tr.Items_Held.add(itemName);
					}
					// if transaction state is blocked then the current operation is added to waiting operations in Transaction table of that transaction.
				} else if (tr.Transaction_State.equals("blocked")) {
					tr.Waiting_Operation.add(line);
					System.out.println("Current transaction is blocked and operation " + line
							+ " is added to waiting Operations");
					System.out.println("Waiting operations of Transaction"+tranId+" are"+tr.Waiting_Operation);
				}
				// if transaction state  is aborted then ignoring the curent operation.
				else {
					System.out.println("Transaction " + tranId + " is already aborted");
				}
				break;
			}
			// Reading 'e' in input line means commiting the current trasction and aborting the transaction by release all lock held by the
				// Transaction and waking all waiting Transaction to perform their operations.
			case "e": {
				String trans_id = line.substring(1, line.length() - 1);
				Transaction tr = (Transaction) TranTable.get(trans_id);
				if (tr.Transaction_State == "Active") {
					// Changing state to committed.
					tr.Transaction_State = "Committed";
					System.out.printf("Transaction %s is committed \n", trans_id);
				}
				// if transaction state is blocked then the current operation is added to waiting operations in Transaction table of that transaction.
				else if (tr.Transaction_State.equals("blocked")) {
					tr.Waiting_Operation.add(line);
					System.out.println("Current transaction is blocked and operation " + line
							+ " is added to waiting Operations");
					System.out.println("Waiting operations of Transaction"+trans_id+" are"+tr.Waiting_Operation);
					break;
				}
				// if transaction state  is aborted then ignoring the curent operation.
				else {
					System.out.println("Transaction " + trans_id + " is already aborted");
					break;
				}
				tr.Items_Held.clear();
				// Removing locks by trans_id in Lock Table
				for (String key : LockTable.keySet()) {
					Lock item = LockTable.get(key);
					// Removing current transaction from Locking Transactions column of
					// item.
					if (item.Locking_Transactions.contains(trans_id)) {
						item.Locking_Transactions.remove(trans_id);
							System.out.printf("Lock held by Transaction %s for item %s is released\n", trans_id,item.Item_Name);
						if (item.Locking_Transactions.isEmpty()) {
							if (item.Waiting_Transactions.isEmpty()) {
								// As there are no Transactions waiting on this item,
								// this item is now UL
								item.Lock_Mode = "UL";
								System.out.println("As there are no waiting transaction item " + item.Item_Name + " is unlocked");
							} else {
								Run_waiting_transactions(item.Waiting_Transactions);
							}
						}
						else{
							Run_waiting_transactions(item.Waiting_Transactions);
						}
					}
					// Remove aborted transaction from Waiting Transactions List
					if (item.Waiting_Transactions.contains(trans_id)) {
						item.Waiting_Transactions.remove(trans_id);
					}
				}
				break;
			}
			default:
				System.out.println("Invalid input");
				break;
		}
	}
// wait_and_die method checks if requesting Transaction is older or not. if older it waits in an younger Transaction otherwise it aborts.
	private static void Wait_and_Die(String line, String itemName, String tranId) {
		Transaction tr = TranTable.get(tranId);
		Lock lck1 = LockTable.get(itemName);
		List write_locked_trans = new ArrayList();
		write_locked_trans = lck1.Locking_Transactions;
		for (int i=0 ;i<write_locked_trans.size();i++)
		{
			Transaction tr2 = TranTable.get(write_locked_trans.get(i));
			if (tr2!=tr)
			{
				int tj = tr2.Timestamp;
				int ti = tr.Timestamp;
				if (ti < tj) {
					System.out.println("Transaction  "+tr.TransactionID + " Waits as Timestamp of "+ tr.TransactionID + " is less than Transaction" + tr2.TransactionID);
					Wait_Transaction(tr.TransactionID, line);
				} else {
					System.out.println("Transaction "+tr.TransactionID + " Aborts as Timestamp of "+ tr.TransactionID + " is greater than Transaction" + tr2.TransactionID);
					Abort_Transaction(tr.TransactionID);
				}
			}
		}
	}
//Blocking the  current trasctions is simply change it's state to blocke and adding the operation to waiting operations in the Transaction table
	private static void Wait_Transaction(String trans_id, String line) {
		Transaction tr3 = TranTable.get(trans_id);
		tr3.Transaction_State = "blocked";
		System.out.println("Current transaction is blocked and operation " + line
				+ " is added to waiting Operations");
		tr3.Waiting_Operation.add(line);
		System.out.println("Waiting operations of Transaction"+trans_id+" are"+tr3.Waiting_Operation);
		String itemName = line.substring(line.indexOf('(') + 1, line.indexOf(')'));
		Lock lck= LockTable.get(itemName);
		lck.Waiting_Transactions.add(trans_id);

	}
//Aborting a Transaction by releasing all locks held by that Transaction and waking up all the waiting Transactions to perform their blocked operations.
	public static void Abort_Transaction(String trans_id) {
		Transaction tr = TranTable.get(trans_id);
		tr.Transaction_State = "aborted";
		try {
			tr.Items_Held.clear();
		}
		catch (Exception e)
		{
//			System.out.println("No items held by this transaction");
		}
		// Removing locks by trans_id in Lock Table
		for (String key : LockTable.keySet()) {
			Lock item = LockTable.get(key);
			// Removing current transaction from Locking Transactions column of
			// item.
			if (item.Locking_Transactions.contains(trans_id) ) {
				item.Locking_Transactions.remove(trans_id);
				System.out.printf("Lock held by Transaction %s for item %s is released\n", trans_id,item.Item_Name);
				if (item.Locking_Transactions.isEmpty()) {
					if (item.Waiting_Transactions.isEmpty()) {
						// As there are no Transactions waiting on this item,
						// this item is now UL
						item.Lock_Mode = "UL";
						System.out.println("As there are no waiting transaction item " + item.Item_Name + " is unlocked");
					} else {
						Run_waiting_transactions(item.Waiting_Transactions);
					}
				}
				else{
					Run_waiting_transactions(item.Waiting_Transactions);
				}
			}

			// Remove aborted transaction from Waiting Transactions List
			if (item.Waiting_Transactions.contains(trans_id)) {

				item.Waiting_Transactions.remove(trans_id);
			}
		}
	}
//Run_waiting_transactions methods simply pass the stored operation of the blocked transaction to parseInput method.
	private static void Run_waiting_transactions(List<String> waiting_Transactions) {
		for (int j = 0; j < waiting_Transactions.size(); j++) {
			// Running all waiting operations of First waiting Transaction.
			System.out.println("Started waiting transaction " + waiting_Transactions.get(j));
			Transaction tr = TranTable.get(waiting_Transactions.get(j));
			System.out.println("Transaction State- "+tr.Transaction_State+" Waiting operation of the transaction "+tr.Waiting_Operation );
			for (Iterator<String> i = tr.Waiting_Operation.iterator(); i.hasNext();) {
				String inpStr = i.next();
				tr.Transaction_State = "Active";
				parseInput(inpStr);
				//tr.Waiting_Operation.remove(inpStr);
			}
		}
	}

}
