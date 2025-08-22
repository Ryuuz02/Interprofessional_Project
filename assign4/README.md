# 📝 **Assignment 4: B+ Tree Index Manager - CS525 Spring 2025**


## Team Members
Akshith Goud Kasipuram (A20561274) - akasipuram@hawk.iit.edu

Rahul Tatikonda (A20546868) - rtatikonda@hawk.iit.edu

Jacob Bode (A20489414) - jbode2@hawk.iit.edu

Prithwee Reddei Patelu (A20560828) - ppatelu@hawk.iit.edu

## Output Video Link

📽️ https://www.loom.com/share/1bb2958b58644d3a8d1797b0dec97650

---

## Contribution Breakdown

Our team collaborated closely and shared responsibilities equally. Each member contributed 25% toward the successful completion of the project.

- Akshith Goud Kasipuram – 25%  
- Rahul Tatikonda        – 25%  
- Jacob Bode             – 25%  
- Prithwee Reddei Patelu – 25%






 ## **RUNNING THE SCRIPT:**
```sh
cd assign4       # Navigate to project directory
ls               # Verify project files
make clean       # Remove old compiled files
make             # Compile all project files
./test_assign4   # Run the main test driver to validate full B+ Tree functionality
./test expr      # Run additional expression tests
```


## 📖 **Project Overview**

### **Goal:**
This assignment completes the implementation of a **Buffer Pool Manager**, **B+-Tree Indexing**, and **Record Manager** as part of the **Database Management System (DBMS)** project. It integrates multiple components to provide efficient memory management, indexing, and record handling in the context of a database.

**Key Objectives**:
1. **Buffer Pool Management**: Implement a system to manage in-memory pages with strategies like **FIFO**, **LRU**, and **CLOCK**.
2. **B+-Tree Indexing**: Implement a **B+-Tree** for efficient **indexing**, which handles operations like **insertion**, **deletion**, and **searching**.
3. **Record Management**: Manage **records** in the database with features like **NULL handling** and **serialization**.

The project builds on the foundations laid in **Assignments 1, 2, and 3**, each of which progressively added components like the **Storage Manager**, **Buffer Manager**, and **Record Manager**, ensuring seamless integration and performance across modules.

---

### **Key Components and Architecture**:

1. **Buffer Pool Management** (`buffer_mgr.c`, `buffer_mgr.h`):
   - Manages in-memory pages with strategies like **FIFO**, **LRU**, and **CLOCK** to improve I/O performance by reducing disk access.
   - Supports operations like **pinning**, **unpinning**, and **eviction** of pages.
   
2. **B+-Tree Indexing** (`btree_mgr.c`, `btree_mgr.h`):
   - Efficiently handles index creation, **key insertion**, **key deletion**, and **searching**. It ensures fast retrieval and efficient memory usage.
   - Includes functionality for **node splitting** and **node merging**, ensuring the B+-Tree remains balanced during modifications.

3. **Record Management** (`record_mgr.c`, `record_mgr.h`):
   - Manages **record-level operations** like **insertion**, **deletion**, and **retrieval**.
   - Includes extended support for **NULL values**, ensuring that records can handle nullable attributes correctly.

4. **Storage Management** (`storage_mgr.c`, `storage_mgr.h`):
   - Handles **disk I/O operations** for reading and writing pages from disk. It ensures data persistence and interacts with the **Buffer Manager** for page loading and saving.

---

### **Building on Previous Assignments**:

This assignment is a natural continuation of **Assignment 1** (Storage Manager) and **Assignment 2** (Buffer Manager), which together laid the foundation for efficient memory management and disk I/O handling:

- **Assignment 1** introduced the **Storage Manager**, handling low-level file I/O operations, and managing **page files** that store records.
- **Assignment 2** focused on the **Buffer Manager**, implementing various **page replacement strategies** to optimize memory usage and reduce disk access.
  
- In **Assignment 3**, we expanded on the **Record Manager**, adding **NULL handling** and integrating it with the **Buffer Manager** to support record-level operations. 

- **Assignment 4** integrates all these components by focusing on the **Buffer Pool** and **Index Management**, combining **B+-Tree** indexing, **Record Management**, and **Disk I/O** to simulate a full-fledged **DBMS**.**

---

## 📂 **Files Overview**:

1. **Source Files**:
   - **`btree_mgr.c`** / **`btree_mgr.h`**: Implements the **B+-Tree index**, supporting operations like key insertion, deletion, and searching.
   - **`buffer_mgr.c`** / **`buffer_mgr.h`**: Implements the **Buffer Manager**, including page pinning, unpinning, and eviction strategies (FIFO, LRU, CLOCK).
   - **`record_mgr.c`** / **`record_mgr.h`**: Implements the **Record Manager**, handling record operations with support for **NULL values**.
   - **`storage_mgr.c`** / **`storage_mgr.h`**: Implements **Storage Manager** functionalities for disk-level page I/O operations.
   - **`dberror.c`** / **`dberror.h`**: Defines error codes and functions for error handling.
   
2. **Test Files**:
   - **`test_assign4_1.c`**: Contains test cases for validating **B+-Tree** operations and integration with **Buffer Manager**.
   - **`test_expr.c`**: Contains tests for evaluating **expressions** involving NULL values.
   - **`test_helper.h`**: Utility functions to support testing (e.g., assertions, error checks).
   
3. **Miscellaneous**:
   - **`Makefile`**: Automates the compilation and testing process, ensuring easy building and clean-up.
   - **`README.md`**: This documentation file, providing a comprehensive overview of the project.

---


### **Detailed Design**:

This project implements a B+ Tree-based indexing module in C. The system supports operations to create, open, and manage B+ Trees for efficient indexing. It interacts with the storage manager and buffer manager to handle persistent data, enabling search, insert, delete, and traversal operations on key-value pairs.


## Features
- B+ Tree creation and destruction
- Insertions with node splitting and key promotion
- Deletions with merging and redistribution
- Key-based search and result retrieval
- In-order traversal using tree scanning
- Tree structure visualization
- Metadata tracking for node and key counts

---

## Key Functions and Responsibilities

### Initialization and Setup
- `initIndexManager` – Initializes the global state and data types.
- `shutdownIndexManager` – Cleans up the index manager.

### Tree Creation and File Handling
- `createBtree` – Creates and initializes a page file for the index.
- `openBtree` – Opens an index file and loads its metadata.
- `closeBtree` – Closes the tree and frees resources.
- `deleteBtree` – Deletes the tree's page file.

### Tree Metadata Operations
- `getNumNodes` – Returns the current number of nodes in the tree.
- `getNumEntries` – Returns the current number of entries.
- `getKeyType` – Returns the data type of keys.

### Insertion Operations
- `insertKey` – Inserts a key and associated RID into the B+ Tree.
- `insertKey_findLeaf` – Finds the leaf node where a key should be inserted.
- `insertKey_initializeRoot` – Creates the root node during the first insert.
- `insertKey_insertInLeafOrSplit` – Chooses between simple insert or splitting.
- `insertKey_insertWithoutSplit` – Inserts into leaf without overflow.
- `insertKey_splitAndInsert` – Splits a full node and inserts the key.

### Parent Handling
- `insertParent` – Inserts key into parent after node split.
- `insertParent_createNewRoot` – Creates a new root after root split.
- `insertParent_insertIntoParent` – Inserts into a parent node.
- `insertParent_splitNode` – Splits a full parent node.

### Search Operations
- `findKey` – Finds a key in the tree and returns its RID.
- `findKey_locateLeafNode` – Traverses tree to find appropriate leaf.
- `findKey_searchInLeaf` – Searches a key in a leaf.
- `findKey_setResultIfFound` – Sets result RID if key is found.

### Deletion Operations
- `deleteKey` – Deletes a key from the tree.
- `deleteKey_findLeafNode` – Locates leaf node for deletion.
- `deleteKey_searchKeyInLeaf` – Finds key index in a leaf.
- `deleteKey_deleteNodeFromLeaf` – Deletes key from the node.
- `deleteNode` – Handles merging/borrowing and rebalancing after deletion.

### Scanning Operations
- `openTreeScan` – Initializes a scan over the tree.
- `nextEntry` – Returns the next entry in scan.
- `closeTreeScan` – Closes and cleans up the scan.
- `initializeScanHandle` – Prepares scan handle.
- `initializeScanMgmtData` – Initializes scan metadata.
- `initializeLeafNode` – Finds leftmost leaf to begin scan.
- `moveToNextLeafIfNeeded` – Jumps to next leaf during scan.
- `retrieveNextRID` – Retrieves current RID from leaf node.

### Tree Visualization
- `printTree` – Prints the B+ Tree in structured format.
- `walk` – Recursive DFS to build the tree representation.
- `walkSubNodes` – Traverses sub-nodes recursively.
- `DFS` – Assigns position numbers to nodes.
- `processNode` – Chooses leaf/internal formatting logic.
- `processLeafNode` – Formats leaf node for print.
- `processInternalNode` – Formats internal node for print.
- `initializeLine` – Prepares a string line for a node.

### Node Management Helpers
- `createNewNod` – Creates and initializes a new node.
- `createNewNod_allocateNode` – Allocates memory for node.
- `createNewNod_initializeNode` – Sets default values for node.
- `createNewNod_setGlobalPos` – Resets global node position counter.
- `strNoLarger` – Compares serialized key strings.

---






## 🏁 **Conclusion**:

In Assignment 4, we combined the Storage Manager, Buffer Manager, and Record Manager built earlier to create a complete database system simulation. It efficiently handles memory, supports B+ Tree indexing, and manages records—even those with NULL values. We ran a series of tests to make sure everything works correctly from end to end.
