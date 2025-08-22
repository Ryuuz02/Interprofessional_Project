#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "record_mgr.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include <ctype.h>
#include "dberror.h"

const int ATR_SIZE = 15; 
RC Return_Code;     // Return code for function execution tracking

typedef struct RecordManager {
    BM_BufferPool buffer_pl;   // Buffer pool for page management
    BM_PageHandle handel_pg;   // Page handler for current page operations
    RID record_ID;             // Stores record identification (page, slot)
    Expr *condition;           // Expression condition for scanning operations
    int tp_count;              // Tracks total number of tuples
    int scan_count;            // Maintains scan iteration progress
    int page_free;             // Tracks available free space within pages
} R_Man;



R_Man *r_Manager;   

extern RC createTable(char *name, Schema *schema) {
    // Indicate successful initialization
    Return_Code = RC_OK;

    // Allocate memory for record manager instance
    r_Manager = (R_Man *) malloc(sizeof(R_Man));

    // Temporary buffer to store schema metadata
    char schemaBuffer[PAGE_SIZE];
    char *bufferPtr = schemaBuffer;

    // Initialize buffer pool and embed metadata into the first page
    do {
        initBufferPool(&r_Manager->buffer_pl, name, 100, RS_LRU, NULL);
        *((int *)bufferPtr) = 0;  
        bufferPtr += sizeof(int);
        *((int *)bufferPtr) = 1;  // Total pages allocated
        bufferPtr += sizeof(int);
        *((int *)bufferPtr) = schema->numAttr; // Number of attributes
        bufferPtr += sizeof(int);
        *((int *)bufferPtr) = schema->keySize; // Size of primary key
        bufferPtr += sizeof(int);
    } while (0);

    // Store attribute information sequentially
    int attributeIndex = 0;
    while (attributeIndex < schema->numAttr) {
        strncpy(bufferPtr, schema->attrNames[attributeIndex], ATR_SIZE);
        bufferPtr += ATR_SIZE;
        *((int *)bufferPtr) = (int) schema->dataTypes[attributeIndex];
        bufferPtr += sizeof(int);
        *((int *)bufferPtr) = (int) schema->typeLength[attributeIndex];
        bufferPtr += sizeof(int);
        attributeIndex++;
    }

    // File management for schema persistence
    SM_FileHandle fileHandle;
    if (createPageFile(name) == RC_OK) {
        openPageFile(name, &fileHandle);
    }

    if (writeBlock(0, &fileHandle, schemaBuffer) == RC_OK) {
        closePageFile(&fileHandle);
    }

    return RC_OK;
}

extern RC initRecordManager(void *mgmtData) {
    // Initialize storage layer before enabling the record manager
    initStorageManager();
    
    // Set return code to success status
    Return_Code = RC_OK;
    
    return Return_Code;
}

extern RC shutdownRecordManager() {
    // Ensure buffer pool is properly closed before cleanup
    shutdownBufferPool(&r_Manager->buffer_pl);
    
    // Deallocate memory and reset pointer reference
    free(r_Manager);
    r_Manager = NULL;
    
    // Indicate successful shutdown
    Return_Code = RC_OK;
    
    return Return_Code;
}

int findFreeSlot(char *data, int recordSize) {
    int totalSlots = PAGE_SIZE / recordSize;  // Compute max records per page
    int temp = 0;  // Index to traverse the page

    // Iterating through slots to find a vacant position
    while (temp < totalSlots) {
        if (data[temp * recordSize] != '+') {
            return temp; // Return first available slot
        }
        temp++;  // Move to the next record slot
    }
    return -1;  // No free slot available
}



extern RC closeTable(RM_TableData *rel) {
    // Ensure the table has been initialized before attempting to close
    if (rel->mgmtData == NULL) {
        return RC_BUFFER_ERROR;
    }

    // Retrieve the record manager instance linked to the table
    R_Man *recordManager = rel->mgmtData;

    // Shutdown the buffer pool to free memory resources
    shutdownBufferPool(&recordManager->buffer_pl);

    // Confirm successful table closure
    Return_Code = RC_OK;

    return Return_Code;
}



extern RC openTable(RM_TableData *rel, char *name) {
    // Ensure table is not already loaded
    if (rel == NULL || name == NULL) {
        return RC_BUFFER_ERROR;
    }

    // Associate table with record manager
    rel->mgmtData = r_Manager;
    rel->name = name;

    // Pin first page to retrieve table metadata
    RC status = pinPage(&r_Manager->buffer_pl, &r_Manager->handel_pg, 0);
    if (status != RC_OK) {
        return status;
    }

    char *pageHandler = (char *) r_Manager->handel_pg.data;
    if (pageHandler == NULL) {
        return RC_ERROR;
    }

    // Extract metadata values from page
    memcpy(&r_Manager->tp_count, pageHandler, sizeof(int));
    pageHandler += sizeof(int);

    memcpy(&r_Manager->page_free, pageHandler, sizeof(int));
    pageHandler += sizeof(int);

    // Retrieve attribute count from schema metadata
    int attributeCount;
    memcpy(&attributeCount, pageHandler, sizeof(int));
    pageHandler += sizeof(int);

    // Allocate memory for schema
    Schema *schemaInstance = (Schema *) malloc(sizeof(Schema));
    if (schemaInstance == NULL) {
        return RC_ERROR;
    }

    schemaInstance->numAttr = attributeCount;
    schemaInstance->dataTypes = (DataType *) malloc(attributeCount * sizeof(DataType));
    schemaInstance->typeLength = (int *) malloc(attributeCount * sizeof(int));
    schemaInstance->attrNames = (char **) malloc(attributeCount * sizeof(char *));

    if (schemaInstance->dataTypes == NULL || schemaInstance->typeLength == NULL || schemaInstance->attrNames == NULL) {
        free(schemaInstance);
        return RC_ERROR;
    }

    // Allocate memory for attribute names and verify allocation
    for (int i = 0; i < attributeCount; i++) {
        schemaInstance->attrNames[i] = (char *) malloc(ATR_SIZE);
        if (schemaInstance->attrNames[i] == NULL) {
            return RC_ERROR;
        }
    }

    // Extract attribute metadata
    for (int i = 0; i < schemaInstance->numAttr; i++) {
        strncpy(schemaInstance->attrNames[i], pageHandler, ATR_SIZE);
        pageHandler += ATR_SIZE;

        memcpy(&schemaInstance->dataTypes[i], pageHandler, sizeof(int));
        pageHandler += sizeof(int);

        memcpy(&schemaInstance->typeLength[i], pageHandler, sizeof(int));
        pageHandler += sizeof(int);
    }

    // Assign schema to table
    rel->schema = schemaInstance;

    // Unpin page and ensure changes are saved
    unpinPage(&r_Manager->buffer_pl, &r_Manager->handel_pg);
    forcePage(&r_Manager->buffer_pl, &r_Manager->handel_pg);

    return RC_OK;
}


extern RC deleteTable(char *name) {
    // Validate the input to ensure a valid table name is provided
    if (name == NULL) {
        return RC_BUFFER_ERROR;
    }

    // Execute the deletion process for the specified table file
    destroyPageFile(name);

    // Return success status after deletion
    return RC_OK;
}


extern int getNumTuples(RM_TableData *rel) {
    // Verify that the table management data is initialized
    if (rel->mgmtData == NULL) {
        return RC_BUFFER_ERROR;
    }

    // Extract the tuple count from the record manager instance
    R_Man *recordManager = rel->mgmtData;
    return recordManager->tp_count;
}



extern RC insertRecord(RM_TableData *rel, Record *record) {
    // Validate that the table is properly initialized
    if (rel->mgmtData == NULL) {
        return RC_SCHEMA_ERROR;
    }

    // Retrieve record manager and initialize variables
    RID *record_ID = &record->id;
    R_Man *recordManager = rel->mgmtData;
    int recordSize = getRecordSize(rel->schema);
    char *pageData, *slotPointer;

    // Assign the page that has available free space
    record_ID->page = recordManager->page_free;
    pinPage(&recordManager->buffer_pl, &recordManager->handel_pg, record_ID->page);
    pageData = recordManager->handel_pg.data;

    // Locate an available slot within the page
    record_ID->slot = findFreeSlot(pageData, recordSize);

    // If no free slot is found, move to the next page and repeat the search
    while (record_ID->slot == -1) {
        unpinPage(&recordManager->buffer_pl, &recordManager->handel_pg);
        record_ID->page++;
        pinPage(&recordManager->buffer_pl, &recordManager->handel_pg, record_ID->page);
        pageData = recordManager->handel_pg.data;
        record_ID->slot = findFreeSlot(pageData, recordSize);
    }

    // Mark page as modified before inserting new data
    markDirty(&recordManager->buffer_pl, &recordManager->handel_pg);

    // Compute the memory location for storing the record
    slotPointer = pageData + (record_ID->slot * recordSize);
    *slotPointer = '+';  // Mark slot as occupied

    // Copy record data into the designated slot
    memcpy(++slotPointer, record->data + 1, recordSize - 1);

    // Unpin the modified page
    unpinPage(&recordManager->buffer_pl, &recordManager->handel_pg);

    // Update the total tuple count and pin metadata page
    recordManager->tp_count++;
    pinPage(&recordManager->buffer_pl, &recordManager->handel_pg, 0);

    return Return_Code;
}



extern RC deleteRecord(RM_TableData *rel, RID id) {
    // Ensure the table is initialized before proceeding
    if (rel->mgmtData == NULL) {
        return RC_SCHEMA_ERROR;
    }

    // Retrieve the record manager instance
    R_Man *recordManager = rel->mgmtData;
    
    // Pin the page where the record is located
    pinPage(&recordManager->buffer_pl, &recordManager->handel_pg, id.page);
    recordManager->page_free = id.page;

    // Calculate the record's memory location
    int recordSize = getRecordSize(rel->schema);
    char *dataPointer = recordManager->handel_pg.data + (id.slot * recordSize);

    // Mark the entire record slot as deleted
    for (int i = 0; i < recordSize; i++) {
        dataPointer[i] = '-';
    }

    // Indicate that the page has been modified
    markDirty(&recordManager->buffer_pl, &recordManager->handel_pg);
    
    // Unpin and force-write the modified page to disk
    unpinPage(&recordManager->buffer_pl, &recordManager->handel_pg);
    forcePage(&recordManager->buffer_pl, &recordManager->handel_pg);

    return RC_OK;
}



extern RC updateRecord(RM_TableData *rel, Record *record) {	
    // Validate that the table is properly initialized
    if (rel->mgmtData == NULL) {
        return RC_SCHEMA_ERROR;
    }

    // Retrieve the record manager instance
    R_Man *recordManager = rel->mgmtData;

    // Identify the page containing the record and pin it for modification
    pinPage(&recordManager->buffer_pl, &recordManager->handel_pg, record->id.page);

    // Obtain the record's memory location
    int recordSize = getRecordSize(rel->schema);
    char *dataPointer = recordManager->handel_pg.data + (record->id.slot * recordSize);

    // Mark the slot as occupied before updating
    *dataPointer = '+';
    dataPointer++;

    // Copy the new record data, excluding the first byte
    memcpy(dataPointer, record->data + 1, recordSize - 1);

    // Indicate that the page has been modified and unpin it
    markDirty(&recordManager->buffer_pl, &recordManager->handel_pg);
    unpinPage(&recordManager->buffer_pl, &recordManager->handel_pg);

    return RC_OK;	
}



extern RC getRecord(RM_TableData *rel, RID id, Record *record) {
    if (rel->mgmtData == NULL) {
        return RC_SCHEMA_ERROR;
    }

  
    R_Man *recordManager = rel->mgmtData;
    Return_Code = RC_OK;
    // Pin the page containing the requested record
    pinPage(&recordManager->buffer_pl, &recordManager->handel_pg, id.page);

    // Compute the memory location of the record within the page
    int recordSize = getRecordSize(rel->schema);
    char *dataPointer = recordManager->handel_pg.data + (id.slot * recordSize);

    // Check if the record exists before copying data
    if (recordSize > 0) {
        if (*dataPointer == '+') {
            record->id = id;
            memcpy(record->data + 1, dataPointer + 1, recordSize - 1);
        } else {
            return RC_RM_NO_TUPLE_WITH_GIVEN_RID;
        }
    }
    unpinPage(&recordManager->buffer_pl, &recordManager->handel_pg);
    return Return_Code;
}


extern RC startScan(RM_TableData *rel, RM_ScanHandle *scan, Expr *condition) {
    R_Man *scanManager;
    R_Man *tableManager;

    // Verify that a condition is provided
    if (condition == NULL) {
        return RC_SCAN_CONDITION_NOT_FOUND;
    }

    // Open the table for scanning
    openTable(rel, "ScanTable");
    Return_Code = RC_OK;

    // Allocate memory for managing the scan
    scanManager = (R_Man *) malloc(sizeof(R_Man));
    if (scanManager == NULL) {
        return RC_ERROR;  // FIXED: Removed RC_MEMORY_ERROR
    }

    // Initialize scan-related metadata
    scan->mgmtData = scanManager;
    scanManager->record_ID.slot = 0;
    scanManager->record_ID.page = 1;
    scanManager->condition = condition;
    scanManager->scan_count = 0;

    // Link the scan handle to the table data
    tableManager = rel->mgmtData;
    tableManager->tp_count = ATR_SIZE;
    scan->rel = rel;

    return Return_Code;
}




extern RC next(RM_ScanHandle *scan, Record *record) {
    Return_Code = RC_OK;
    if (scan->mgmtData == NULL) {
        return RC_SCAN_CONDITION_NOT_FOUND;
    }

    // Retrieve necessary manager instances
    R_Man *scanManager = scan->mgmtData;
    R_Man *tableManager = scan->rel->mgmtData;
    Schema *schema = scan->rel->schema;

    // Ensure a valid scan condition exists
    if (scanManager->condition == NULL) {
        return RC_SCAN_CONDITION_NOT_FOUND;
    }

    // Allocate memory for evaluating the condition
    Value *result = (Value *) malloc(sizeof(Value));

    // Prepare scan parameters
    char *dataPointer;
    int totalSlots = PAGE_SIZE / getRecordSize(schema);
    int scanCount = scanManager->scan_count;
    int totalTuples = tableManager->tp_count;

    // If there are no tuples, return appropriate status
    if (totalTuples == 0) {
        return RC_RM_NO_MORE_TUPLES;
    }

    // Iterate over records to find the next matching tuple
    do {
        // Move to the next slot or page if necessary
        scanManager->record_ID.slot++;
        if (scanManager->record_ID.slot >= totalSlots) {
            scanManager->record_ID.page++;
            scanManager->record_ID.slot = 0;
        }

        // Pin the page containing the current record
        pinPage(&tableManager->buffer_pl, &scanManager->handel_pg, scanManager->record_ID.page);
        dataPointer = scanManager->handel_pg.data + (scanManager->record_ID.slot * getRecordSize(schema));

        // Store record details
        record->id.slot = scanManager->record_ID.slot;
        record->id.page = scanManager->record_ID.page;
        char *recordData = record->data;
        *recordData = '-';
        memcpy(++recordData, dataPointer + 1, getRecordSize(schema) - 1);

        // Increment scan count and evaluate expression
        scanManager->scan_count++;
        scanCount++;
        evalExpr(record, schema, scanManager->condition, &result);

        // If the condition is met, return the record
        if (result->v.boolV == TRUE) {
            unpinPage(&tableManager->buffer_pl, &scanManager->handel_pg);
            return Return_Code;
        }
    } while (scanCount <= totalTuples);
    unpinPage(&tableManager->buffer_pl, &scanManager->handel_pg);
    scanManager->record_ID.slot = 0;
    scanManager->record_ID.page = 1;
    scanManager->scan_count = 0;

    return RC_RM_NO_MORE_TUPLES;
}


extern RC closeScan(RM_ScanHandle *scan) {
    if (scan->mgmtData == NULL) {
        return RC_BUFFER_ERROR;
    }

    R_Man *tableManager = scan->rel->mgmtData;
    R_Man *scanManager = scan->mgmtData;

    if (scanManager->scan_count > 0) {
        unpinPage(&tableManager->buffer_pl, &scanManager->handel_pg);

        scanManager->scan_count = 0;
        scanManager->record_ID.page = 1;
        scanManager->record_ID.slot = 0;
    }

    free(scan->mgmtData);
    scan->mgmtData = NULL;

    return RC_OK;
}


extern int getRecordSize(Schema *schema) {
    int recordBytes = 1;  

    for (int attrIdx = 0; attrIdx < schema->numAttr; attrIdx++) {
        // Determine memory allocation for the attribute type
        if (schema->dataTypes[attrIdx] == DT_STRING) {
            recordBytes += schema->typeLength[attrIdx];
        } else if (schema->dataTypes[attrIdx] == DT_INT) {
            recordBytes += sizeof(int);
        } else if (schema->dataTypes[attrIdx] == DT_FLOAT) {
            recordBytes += sizeof(float);
        } else if (schema->dataTypes[attrIdx] == DT_BOOL) {
            recordBytes += sizeof(bool);
        }
    }

    return recordBytes;
}



extern Schema *createSchema(int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys) {
    switch (keySize > 0) {
        case 0:
            return NULL;
    }
    Schema *newSchema = (Schema *) malloc(sizeof(Schema));

 
    newSchema->keySize = keySize;
    newSchema->numAttr = numAttr;
    newSchema->keyAttrs = keys;
    newSchema->attrNames = attrNames;
    newSchema->typeLength = typeLength;
    newSchema->dataTypes = dataTypes;

    return newSchema;
}


extern RC createRecord(Record **record, Schema *schema) {
    Return_Code = RC_OK;

    Record *newRecord = (Record *) malloc(sizeof(Record));
    newRecord->data = (char *) malloc(getRecordSize(schema));

    // Set initial values for record ID
    newRecord->id.page = -1;
    newRecord->id.slot = -1;

    // Initialize the record's data section
    char *dataPointer = newRecord->data;
    *dataPointer = '-';
    *(++dataPointer) = '\0';

    *record = newRecord;

    return Return_Code;
}

extern RC freeSchema(Schema *schema) {
    if (schema != NULL) {
        free(schema);
    }
    return RC_OK;
}

RC attrOffset (Schema *schema, int attrNum, int *result)
{   
    Return_Code = RC_OK;
    int i=0;
    *result = 1;
	if(attrNum>=0){
		loop:
		switch(schema->dataTypes[i]){
			case DT_INT:
				if(i<attrNum)
					*result=sizeof(int)+*result;
				break;
			case DT_STRING:
				if(i<attrNum)
					*result = *result + (*schema).typeLength[i];
				break;
			case DT_BOOL:
				if(i<attrNum)
					*result=sizeof(bool)+*result;
				break;			
			case DT_FLOAT:
				if(i<attrNum)
					*result=sizeof(float)+*result;
				break;			
		}
		i=i+1;
		if(i<attrNum)
			goto loop;

	}
	return Return_Code;
}

extern RC freeRecord(Record *record) {
    int deallocFlag = 0;
    
    while (1) {
        deallocFlag = 1;
        break;
    }

    // Perform memory deallocation if the flag is set
    if (deallocFlag) {
        free(record);
    }

    return RC_OK;
}


extern RC getAttr(Record *record, Schema *schema, int attrNum, Value **value) {

    if (attrNum < 0) {
        return RC_SCHEMA_ERROR;
    }
    int position = 0;
    attrOffset(schema, attrNum, &position);
    Value *attribute = (Value *) malloc(sizeof(Value));
    if (attribute == NULL) {
        return RC_ERROR;
    }
    char *dataPointer = record->data + position;
    if (position != 0) {
        if (attrNum == 1) {
            schema->dataTypes[attrNum] = 1;
        } else {
            schema->dataTypes[attrNum] = schema->dataTypes[attrNum];
        }
    }
    
    
    
    
    
    
    

    if (position != 0) {
        switch (schema->dataTypes[attrNum]) {
            case DT_STRING: {
                if (position != 0) {
                    int length = schema->typeLength[attrNum];
                    attribute->v.stringV = (char *) malloc(length + 1);
                    strncpy(attribute->v.stringV, dataPointer, length);
                    if (position != 0) {
                        attribute->v.stringV[length] = '\0';
                    }
                    attribute->dt = DT_STRING;
                }
                break;
            }
            case DT_INT: {
                if (position != 0) {
                    int intValue = 0;
                    memcpy(&intValue, dataPointer, sizeof(int));
                    if (position != 0) {
                        attribute->dt = DT_INT;
                    }
                    attribute->v.intV = intValue;
                }
                break;
            }
            case DT_FLOAT: {
                if (position != 0) {
                    float floatValue;
                    memcpy(&floatValue, dataPointer, sizeof(float));
                    if (position != 0) {
                        attribute->dt = DT_FLOAT;
                    }
                    attribute->v.floatV = floatValue;
                }
                break;
            }
            case DT_BOOL:
                break;
        }
    }

    // Handle boolean attribute separately
    if (schema->dataTypes[attrNum] == DT_BOOL) {
        bool boolValue;
        memcpy(&boolValue, dataPointer, sizeof(bool));
        if (position != 0) {
            attribute->v.boolV = boolValue;
        }
        attribute->dt = DT_BOOL;
    }

    // Assign the retrieved value to output
    *value = attribute;
    return RC_OK;
}

extern RC setAttr(Record *record, Schema *schema, int attrNum, Value *value) {
    int position = 0;
    Return_Code = RC_OK;
    if (attrNum >= 0) {	
 
        attrOffset(schema, attrNum, &position);

        char *dataPointer = record->data + position;

        switch (schema->dataTypes[attrNum]) {
            case DT_STRING:
                if (attrNum >= 0) {
                    strncpy(dataPointer, value->v.stringV, schema->typeLength[attrNum]);
                    dataPointer += schema->typeLength[attrNum];
                }
                break;
            case DT_INT:
                if (attrNum >= 0) {
                    *(int *)dataPointer = value->v.intV;	  
                    dataPointer += sizeof(int);
                }
                break;
            case DT_FLOAT:
                if (attrNum >= 0) {
                    *(float *)dataPointer = value->v.floatV;
                    dataPointer += sizeof(float);
                }
                break;
            case DT_BOOL:
                if (attrNum >= 0) {
                    *(bool *)dataPointer = value->v.boolV;
                    dataPointer += sizeof(bool);
                }
                break;
        }
    }
    
    return Return_Code;
}
