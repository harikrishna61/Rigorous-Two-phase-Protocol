Names:
Harikrishna Bathala (1001415489)
Rohit Katta (1001512896)


==============================================================================================
Each File Explained:

Two_phase_locking.java
Main program file. Contains main class. Input file can be given as command line input 
or cann be hardcoded in main method.

Transaction.java
Class file for Transaction Table. 

Lock.java
Class file for Lock table.

Output-Project1-HariKrishna-Rohit.pdf
Contains output for 5 inputs provided.
==============================================================================================
How Code is Structured:

Transaction Table: Hashmap is used to insert and update this table with key as
Transaction ID and Value pair is Transaction Object. The fields to be stored are
Transaction_ID(Integer), Transaction_timestamp(Integer), Transaction_state (String
- active, locked/waiting, aborted/cancelled, committed), Items_Held(List of
Items),Waiting_operations (List of operations of the blocked transactions).

Lock Table: Hashmap is used to insert and update this table with Key as Item name
and value pair Lock Object. The fields to be stored are, Item_name(String),
Lock_Mode(String -read/share locked, write/exclusive lock),
Locking_Transactions(List of Transaction ID’s), Waiting_Transactions(Priority
Queue)

Two_phase_locking main method will read input file each line and send formatted line to ParseInput
method. Here using switch cases for each operation we write releavant output to command line.

==============================================================================================
How to Run the code:
javac Two_phase_locking.java Transaction.java Lock.java

java Two_phase_locking [input file path]
(OR)
Change the input file path in main method. Now can run without input path as parameter.
java Two_phase_locking

==============================================================================================
