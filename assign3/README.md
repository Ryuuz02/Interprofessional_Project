# **Record Manager - README**

## **Team Members**
- **Akshith Goud Kasipuram** (A20561274) - akasipuram@hawk.iit.edu
- **Rahul Tatikonda** (A20546868) - rtatikonda@hawk.iit.edu
- **Jacob Bode** (A20489414) - jbode2@hawk.iit.edu
- **Prithwee Reddei Patelu** (A20560828) - ppatelu@hawk.iit.edu

## **Output Video Link(Loom):**
 https://www.loom.com/share/3fe04c5aaeaf4cb382350f1d9024f569?sid=f7f7fb43-1031-4281-add8-f5d915e95424

 ## **Contribution Breakdown:**

Our team worked on every part of the project together. Everyone did an equal share of the work.

•  Akshith Goud Kasipuram  - 25%

•  Rahul Tatikonda         - 25%

•  Jacob Bode              - 25%

• Prithwee reddei patelu   - 25%



 ## **RUNNING THE SCRIPT:**
```sh
cd assign3       # Navigate to project directory
ls               # Verify project files
make clean       # Remove old compiled files
make             # Compile all project files
make run         # Run test suite
make test_expr   # Compile expression tests
make run_expr    # Execute expression tests
```

## **Overview**
The Record Manager facilitates database operations such as table creation, record insertion, updates, deletion, and scanning. It efficiently manages structured data using buffer pools and schema-based storage.

## **Core Functionalities**
### **Table Management**
- `createTable(name, schema)`: Initializes a table and its schema.
- `deleteTable(name)`: Removes a table from storage.
- `openTable(rel, name)`: Loads a table into memory.
- `closeTable(rel)`: Closes a table and releases resources.
- `getNumTuples(rel)`: Returns the number of records in a table.

### **Record Handling**
- `insertRecord(rel, record)`: Inserts a new record in the table.
- `deleteRecord(rel, id)`: Marks a record as deleted.
- `updateRecord(rel, record)`: Updates an existing record.
- `getRecord(rel, id, record)`: Fetches a record using its ID.

### **Scanning & Queries**
- `startScan(rel, scan, condition)`: Starts scanning records based on a condition.
- `next(scan, record)`: Retrieves the next matching record.
- `closeScan(scan)`: Ends the scan and releases resources.

### **Schema Management**
- `getRecordSize(schema)`: Computes the size of a record.
- `createSchema(numAttr, attrNames, dataTypes, typeLength, keySize, keys)`: Defines table structure.
- `freeSchema(schema)`: Frees allocated schema memory.

### **Record Attributes**
- `createRecord(record, schema)`: Allocates memory for a new record.
- `freeRecord(record)`: Releases allocated record memory.
- `getAttr(record, schema, attrNum, value)`: Retrieves a specific attribute from a record.
- `setAttr(record, schema, attrNum, value)`: Updates an attribute within a record.

### **Utility Functions**
- `attrOffset(schema, attrNum, result)`: Computes byte offset of an attribute.
- `findFreeSlot(data, recordSize)`: Locates the next available slot for a record.

## **Error Handling**
- `RC_OK`: Operation successful
- `RC_ERROR`: General failure
- `RC_BUFFER_ERROR`: Buffer-related issue
- `RC_SCHEMA_ERROR`: Schema-related error
- `RC_RM_NO_TUPLE_WITH_GIVEN_RID`: Record not found
- `RC_RM_NO_MORE_TUPLES`: No more matching records in scan

## **Conclusion**
This Record Manager efficiently manages structured data through schema-based record storage and buffer management, ensuring optimized database operations and resource allocation.

