#include <string.h>
#include <stdlib.h>
#include "record_mgr.h"
#include "tables.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "test_helper.h"
// #include "rm_serializer.c"

RM_TableData *tableData = NULL;

#include <stdio.h> // For printf

#include <stdio.h> // For printf

// Subfunction to print buffer pool information
void printBufferPoolInfo(BM_BufferPool *bm, int pageNum)
{
    printf("Buffer Pool (bm) info:\n");
    printf("  Page File: %s\n", bm->pageFile);            // Print the name of the page file
    printf("  Number of Pages: %d\n", bm->numPages);      // Print the number of pages
    printf("  Replacement Strategy: %d\n", bm->strategy); // Print the replacement strategy
    printf("  pageNum: %d\n", pageNum);
}

// Subfunction to update page data
void updatePageData(BM_PageHandle *page, Record *record, int slot, int offslot)
{
    switch (1)
    {
    // Adjust the data pointer based on the slot and offset
    case 1:
        page->data += slot * offslot;
        break;
    }

    switch (1)
    {
    // Copy data from record to page
    case 1:
        memcpy(page->data, record->data, offslot);
        break;
    }

    switch (1)
    {
    // Revert the data pointer back to its original position
    case 1:
        page->data -= slot * offslot;
        break;
    }
}

// Subfunction to pin a page
RC pinPageHelper(BM_BufferPool *bm, BM_PageHandle *page, int pageNum)
{
    RC rc = pinPage(bm, page, pageNum);

    switch (rc)
    {
    case RC_OK:
        break;
    default:
        return rc;
    }

    return RC_OK;
}

// Subfunction to unpin and force the page to disk
RC unpinAndForcePage(BM_BufferPool *bm, BM_PageHandle *page, int pageNum)
{
    RC rc;

    // Unpin the page after modifications
    rc = unpinPage(bm, page, pageNum);

    switch (rc)
    {
    case RC_OK:
        break;
    default:
        return rc;
    }

    // Force the page to disk
    rc = forcePage(bm, page, pageNum);
    switch (rc)
    {
    case RC_OK:
        break;
    default:
        return rc;
    }

    return RC_OK;
}

// Subfunction to initialize table management data
RM_tableData_mgmtData *initializeTableMgm()
{
    RM_tableData_mgmtData *tableMgm;

    switch (1)
    {
    // Allocate memory for RM_tableData_mgmtData
    case 1:
        tableMgm = (RM_tableData_mgmtData *)malloc(sizeof(RM_tableData_mgmtData));
        break;
    }

    switch (1)
    {
    // Initialize numPages to 0
    case 1:
        tableMgm->numPages = 0;
        break;
    }

    switch (1)
    {
    // Initialize numRecords to 0
    case 1:
        tableMgm->numRecords = 0;
        break;
    }

    switch (1)
    {
    // Initialize numRecordsPerPage to 0
    case 1:
        tableMgm->numRecordsPerPage = 0;
        break;
    }

    switch (1)
    {
    // Initialize numInsert to 0
    case 1:
        tableMgm->numInsert = 0;
        break;
    }

    return tableMgm;
}

// Subfunction to create the page file and handle errors
RC createPageFileAndCheck(char *name)
{
    RC rc = createPageFile(name);
    switch (rc)
    {
    case RC_OK:
        break;
    default:
        return rc;
    }
    return RC_OK;
}

// Main function: doRecord
RC doRecord(Record *record)
{
    switch ((record == NULL) ? 1 : 0)
    {
    case 1:
        return RC_RM_UNKOWN_DATATYPE;
    case 0:
        break;
    }

    RC rc;
    int pageNum;
    int slot;
    BM_BufferPool *bm;
    BM_PageHandle *page;
    int offslot;

    switch (1)
    {
    // Initialize RC variable
    case 1:
        rc = RC_OK; // Assuming RC_OK as a placeholder value
        break;
    }

    switch (1)
    {
    // Assign the page number from the record's ID
    case 1:
        pageNum = record->id.page;
        break;
    }

    switch (1)
    {
    // Assign the slot from the record's ID
    case 1:
        slot = record->id.slot;
        break;
    }

    switch (1)
    {
    // Retrieve the buffer pool from table management data
    case 1:
        bm = ((RM_tableData_mgmtData *)tableData->mgmtData)->bm;
        break;
    }

    switch (1)
    {
    // Allocate memory for the page handle
    case 1:
        page = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));
        break;
    }

    switch (1)
    {
    // Get the record size from the schema
    case 1:
        offslot = getRecordSize(tableData->schema);
        break;
    }
    page->data = (char *)malloc(PAGE_SIZE);

    // Pin the page
    rc = pinPageHelper(bm, page, pageNum);
    switch (rc)
    {
    case RC_OK:
        break;
    default:
        return rc;
    }


    // Update the page data at the slot
    updatePageData(page, record, slot, offslot);
    rc = markDirty(bm, page, pageNum);

    switch (rc)
    {
    case RC_OK:
        break;
    default:
        return rc;
    }

    // Unpin and force the page to disk
    rc = unpinAndForcePage(bm, page, pageNum);
    switch (rc)
    {
    case RC_OK:
        break;
    default:
        return rc;
    }

    return RC_OK;
}

// -------------------------table and manager

RC initRecordManager(void *mgmtData)
{
    // Allocate memory for table data and assign management data
    switch (1)
    {
    case 1:
        tableData = (RM_TableData *)malloc(sizeof(RM_TableData));
        tableData->mgmtData = mgmtData;
        break;
    }

    return RC_OK;
}

// Subfunction to free the schema if it exists
void freeSchemaIfExists()
{
    switch ((tableData->schema != NULL) ? 1 : 0)
    {
    case 1:
        free(tableData->schema);
        tableData->schema = NULL;
        break;
    case 0:
        break;
    }
}

// Subfunction to free table management data
void freeMgmtData()
{
    free(tableData->mgmtData);
    tableData->mgmtData = NULL;
}

// Subfunction to free the table data itself
void freeTableData()
{
    free(tableData);
    tableData = NULL;
}

// Main function: shutdownRecordManager
RC shutdownRecordManager()
{
    // Free schema if it exists
    freeSchemaIfExists();

    // Free management data and table data
    freeMgmtData();
    freeTableData();

    return RC_OK;
}

// Subfunction to initialize the buffer pool and open the page file
RC initializeBufferPoolAndOpenFile(BM_BufferPool **bm, char *name)
{
    *bm = MAKE_POOL(); // Use double pointer to modify the original bm
    (*bm)->mgmtData = malloc(sizeof(BufferManager));
    RC rc = openPageFile(name, &(*bm)->fH);
    switch (rc)
    {
    case RC_OK:
        break;
    default:
        return rc;
    }
    return RC_OK;
}

// Subfunction to write the first page to disk
RC writeFirstPageToDisk(BM_BufferPool *bm, RM_tableData_mgmtData *tableMgm, Schema *schema)
{
    char *firstPage;
    char *pagePtr;
    RC rc;

    switch (1)
    {
    // Allocate memory for the first page
    case 1:
        firstPage = (char *)malloc(PAGE_SIZE);
        break;
    }

    switch (1)
    {
    // Set the page pointer to the beginning of the page
    case 1:
        pagePtr = firstPage;
        break;
    }

    switch (1)
    {
    // Copy numPages to the page
    case 1:
        memcpy(pagePtr, &tableMgm->numPages, sizeof(int));
        pagePtr += sizeof(int);
        break;
    }

    switch (1)
    {
    // Copy numRecords to the page
    case 1:
        memcpy(pagePtr, &tableMgm->numRecords, sizeof(int));
        pagePtr += sizeof(int);
        break;
    }

    switch (1)
    {
    // Copy numRecordsPerPage to the page
    case 1:
        memcpy(pagePtr, &tableMgm->numRecordsPerPage, sizeof(int));
        pagePtr += sizeof(int);
        break;
    }

    switch (1)
    {
    // Copy numInsert to the page
    case 1:
        memcpy(pagePtr, &tableMgm->numInsert, sizeof(int));
        pagePtr += sizeof(int);
        break;
    }

    switch (1)
    {
    // Append serialized schema to the page
    case 1:
        strcat(pagePtr, serializeSchema(schema));
        break;
    }

    switch (getBlockPos(&bm->fH))
    {
    // Write the first page if the block position is 0
    case 0:
        rc = writeCurrentBlock(&bm->fH, firstPage);
        break;
    // Write the first page at block 0 for non-zero block positions
    default:
        rc = writeBlock(0, &bm->fH, firstPage);
        break;
    }

    switch (1)
    {
    // Free the allocated memory for the first page
    case 1:
        free(firstPage);
        break;
    }

    return rc;
}

// Main createTable function
RC createTable(char *name, Schema *schema)
{
    // Check for null parameters
    switch ((name == NULL) ? 1 : 0)
    {
    case 1:
        return RC_FILE_NOT_FOUND;
    case 0:
        break;
    }

    switch ((schema == NULL) ? 1 : 0)
    {
    case 1:
        return RC_RM_UNKOWN_DATATYPE;
    case 0:
        break;
    }

    // Initialize table management data
    RM_tableData_mgmtData *tableMgm = initializeTableMgm();

    // Create page file
    RC rc = createPageFileAndCheck(name);
    switch (rc)
    {
    case RC_OK:
        break;
    default:
        return rc;
    }

    // Initialize buffer pool and open the page file
    BM_BufferPool *bm = NULL;
    rc = initializeBufferPoolAndOpenFile(&bm, name); // Pass pointer by reference
    switch (rc)
    {
    case RC_OK:
        break;
    default:
        return rc;
    }

    // Write the first page to disk
    rc = writeFirstPageToDisk(bm, tableMgm, schema);
    switch (rc)
    {
    case RC_OK:
        break;
    default:
        return rc;
    }

    // Finalize table management data
    tableMgm->bm = bm;
    tableData->name = name;
    tableData->schema = schema;
    tableData->mgmtData = tableMgm;

    return RC_OK;
}

// Subfunction to initialize the buffer pool for a table
RC initializeBufferPool(RM_TableData *tableData, char *name)
{
    RC rc = initBufferPool(((RM_tableData_mgmtData *)tableData->mgmtData)->bm, name, 10000, RS_CLOCK, NULL);
    switch (rc)
    {
    case RC_OK:
        break;
    default:
        return rc;
    }
    return RC_OK;
}

// Main openTable function
RC openTable(RM_TableData *rel, char *name)
{
    // Check if the name or tableData is NULL
    switch ((name == NULL) ? 1 : 0)
    {
    case 1:
        return RC_FILE_NOT_FOUND;
    case 0:
        break;
    }

    switch ((tableData == NULL) ? 1 : 0)
    {
    case 1:
        return RC_RM_UNKOWN_DATATYPE;
    case 0:
        break;
    }

    // Initialize buffer pool for the table
    RC rc = initializeBufferPool(tableData, name);
    switch (rc)
    {
    case RC_OK:
        break;
    default:
        return rc;
    }

    // Assign tableData to the provided rel structure
    *rel = *tableData;
    return RC_OK;
}

// Subfunction to shutdown the buffer pool for a table
RC shutdownTableBufferPool(RM_TableData *tableData)
{
    RC rc = shutdownBufferPool(((RM_tableData_mgmtData *)tableData->mgmtData)->bm);
    switch (rc)
    {
    case RC_OK:
        break;
    default:
        return rc;
    }
    return RC_OK;
}

// Main closeTable function
RC closeTable(RM_TableData *rel)
{
    // Check if tableData is NULL
    switch ((tableData == NULL) ? 1 : 0)
    {
    case 1:
        return RC_RM_UNKOWN_DATATYPE;
    case 0:
        break;
    }

    // Shutdown the buffer pool for the table
    RC rc = shutdownTableBufferPool(tableData);
    switch (rc)
    {
    case RC_OK:
        break;
    default:
        return rc;
    }

    // Reset the table name in rel
    rel->name = NULL;
    return RC_OK;
}

// Main deleteTable function
RC deleteTable(char *name)
{
    // Check if the name is NULL
    switch ((name == NULL) ? 1 : 0)
    {
    case 1:
        return RC_FILE_NOT_FOUND;
    case 0:
        break;
    }

    // Destroy the page file
    RC rc = destroyPageFile(name);
    switch (rc)
    {
    case RC_OK:
        break;
    default:
        return rc;
    }

    return RC_OK;
}

// Main getNumTuples function
int getNumTuples(RM_TableData *rel)
{
    return ((RM_tableData_mgmtData *)tableData->mgmtData)->numRecords;
}

// Subfunction to calculate the number of records per page
void calculateRecordsPerPage(RM_tableData_mgmtData *tableMgm, int offslot)
{
    tableMgm->numRecordsPerPage = PAGE_SIZE / offslot;
}

// Subfunction to update record management data
void updateRecordManagementData(RM_tableData_mgmtData *tableMgm)
{
    tableMgm->numRecords += 1;
    tableMgm->numInsert += 1;
}

// Subfunction to calculate the slot for a record
int calculateSlot(RM_tableData_mgmtData *tableMgm)
{
    return tableMgm->numInsert % tableMgm->numRecordsPerPage;
}

// Subfunction to assign page and slot for a record
void assignRecordPageAndSlot(Record *record, RM_tableData_mgmtData *tableMgm, int slot)
{
    switch (1)
    {
    // Assign the page number from tableMgm
    case 1:
        record->id.page = tableMgm->numPages;
        break;
    }

    switch ((slot == 0) ? 1 : 0)
    {
    // If slot is 0, assign the last slot of the page
    case 1:
        record->id.slot = tableMgm->numRecordsPerPage - 1;
        break;
    // Otherwise, assign the slot value minus 1
    case 0:
        record->id.slot = slot - 1;
        break;
    }
}

// Main insertRecord function
RC insertRecord(RM_TableData *rel, Record *record)
{
    // Check if rel or record is NULL
    switch ((rel == NULL) ? 1 : 0)
    {
    case 1:
        return -1;
    case 0:
        break;
    }

    switch ((record == NULL) ? 1 : 0)
    {
    case 1:
        return -1;
    case 0:
        break;
    }

    RC rc;

    // Get record size (offslot)
    int offslot = getRecordSize(rel->schema);

    RM_tableData_mgmtData *tableMgm = (RM_tableData_mgmtData *)rel->mgmtData;

    // Calculate the number of records per page
    calculateRecordsPerPage(tableMgm, offslot);

    // Update record management data
    updateRecordManagementData(tableMgm);

    // Calculate the slot for the current record
    int slot = calculateSlot(tableMgm);

    // If slot is 0, increment the page count
    switch ((slot == 0) ? 1 : 0)
    {
    case 1:
        tableMgm->numPages += 1;
        break;
    case 0:
        break;
    }


    // Assign the page and slot to the record
    assignRecordPageAndSlot(record, tableMgm, slot);

    // Call the doRecord function to handle the record insertion
    rc = doRecord(record);
    switch (rc)
    {
    case RC_OK:
        break;
    default:
        return rc;
    }

    // Update the management data
    rel->mgmtData = tableMgm;

    return RC_OK;
}

// Subfunction to mark the page as dirty
RC markPageDirty(BM_BufferPool *bm, BM_PageHandle *page, int pageNum)
{
    RC rc = markDirty(bm, page, pageNum);
    switch (rc)
    {
    case RC_OK:
        break;
    default:
        return rc;
    }
    return RC_OK;
}

// Subfunction to clear data in the record's slot
void clearRecordSlot(BM_PageHandle *page, int slot, int offslot)
{
    page->data += (offslot)*slot;
    memset(page->data, 0, offslot);
    page->data -= (offslot)*slot;
}

// Main deleteRecord function
RC deleteRecord(RM_TableData *rel, RID id)
{
    // Check if rel is NULL
    switch ((rel == NULL) ? 1 : 0)
    {
    case 1:
        return -1;
    case 0:
        break;
    }

    int pageNum;
    int slot;
    RC rc;
    int offslot;
    RM_tableData_mgmtData *tableMgm;
    BM_BufferPool *bm;
    BM_PageHandle *page;

    switch (1)
    {
    // Assign page number from the record's ID
    case 1:
        pageNum = id.page;
        break;
    }

    switch (1)
    {
    // Assign slot from the record's ID
    case 1:
        slot = id.slot;
        break;
    }

    switch (1)
    {
    // Initialize RC variable
    case 1:
        rc = RC_OK; // Placeholder value, change it accordingly
        break;
    }

    switch (1)
    {
    // Get the offset for the record size
    case 1:
        offslot = getRecordSize(tableData->schema);
        break;
    }

    switch (1)
    {
    // Retrieve table management data
    case 1:
        tableMgm = (RM_tableData_mgmtData *)tableData->mgmtData;
        break;
    }

    switch (1)
    {
    // Retrieve buffer manager from table management data
    case 1:
        bm = tableMgm->bm;
        break;
    }

    switch (1)
    {
    // Allocate memory for the page handle
    case 1:
        page = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));
        break;
    }

    switch (1)
    {
    // Allocate memory for the page's data
    case 1:
        page->data = (char *)malloc(PAGE_SIZE);
        break;
    }

    // Pin the page using buffer manager
    rc = pinPageHelper(bm, page, pageNum);
    switch (rc)
    {
    case RC_OK:
        break;
    default:
        return rc;
    }

    // Mark the page as dirty
    rc = markPageDirty(bm, page, pageNum);
    switch (rc)
    {
    case RC_OK:
        break;
    default:
        return rc;
    }

    // Clear the record slot data
    clearRecordSlot(page, slot, offslot);

    // Mark the page dirty again after modification
    rc = markPageDirty(bm, page, pageNum);
    switch (rc)
    {
    case RC_OK:
        break;
    default:
        return rc;
    }

    // Unpin and force the page to disk
    rc = unpinAndForcePage(bm, page, pageNum);
    switch (rc)
    {
    case RC_OK:
        break;
    default:
        return rc;
    }

    switch (1)
    {
    // Decrement the number of records
    case 1:
        tableMgm->numRecords -= 1;
        break;
    }

    switch (1)
    {
    // Update the table management data in tableData
    case 1:
        tableData->mgmtData = tableMgm;
        break;
    }

    switch (1)
    {
    // Copy the updated tableData to the relationship (rel)
    case 1:
        *rel = *tableData;
        break;
    }

    return RC_OK;
}

// Subfunction to validate input parameters
RC validateInput(RM_TableData *rel, Record *record)
{
    switch ((rel == NULL) ? 1 : 0)
    {
    case 1:
        return -1;
    case 0:
        break;
    }

    switch ((record == NULL) ? 1 : 0)
    {
    case 1:
        return -1;
    case 0:
        break;
    }

    return RC_OK;
}

// Subfunction to handle record update
RC processRecordUpdate(Record *record)
{
    RC rc = doRecord(record);
    switch (rc)
    {
    case RC_OK:
        break;
    default:
        return rc;
    }

    return RC_OK;
}

// Main updateRecord function
RC updateRecord(RM_TableData *rel, Record *record)
{
    RC rc = validateInput(rel, record);
    switch (rc)
    {
    case RC_OK:
        break;
    default:
        return rc;
    }

    rc = processRecordUpdate(record);
    switch (rc)
    {
    case RC_OK:
        break;
    default:
        return rc;
    }

    *rel = *tableData;
    return RC_OK;
}

// Subfunction to validate input parameters
RC validateGetRecordInput(RM_TableData *rel, Record *record)
{
    switch ((rel == NULL || record == NULL) ? 1 : 0)
    {
    case 1:
        return RC_RM_UNKOWN_DATATYPE;
    case 0:
        break;
    }
    return RC_OK;
}

// Subfunction to unpin the page
RC unpinPageHelper(BM_BufferPool *bm, BM_PageHandle *page, int pageNum)
{
    RC rc = unpinPage(bm, page, pageNum);
    switch (rc)
    {
    case RC_OK:
        break;
    default:
        return rc;
    }
    return RC_OK;
}

// Subfunction to copy data from the page to the record
void copyDataToRecord(Record *record, BM_PageHandle *page, int slot, int offslot)
{
    switch (1)
    {
    // Move the page data pointer to the correct slot
    case 1:
        page->data += (offslot * slot);
        break;
    }

    switch (1)
    {
    // Copy data from the page to the record
    case 1:
        memcpy(record->data, page->data, offslot);
        break;
    }

    switch (1)
    {
    // Restore the page data pointer to its original position
    case 1:
        page->data -= (offslot * slot);
        break;
    }
}

// Main getRecord function
RC getRecord(RM_TableData *rel, RID id, Record *record)
{
    // Validate input parameters
    RC rc = validateGetRecordInput(rel, record);
    switch (rc)
    {
    case RC_OK:
        break;
    default:
        return rc;
    }

    int pageNum;
    int slot;
    int offslot;
    RM_tableData_mgmtData *temp;
    BM_BufferPool *bm;
    BM_PageHandle *page;

    switch (1)
    {
    // Assign page number from the record's ID
    case 1:
        pageNum = id.page;
        break;
    }

    switch (1)
    {
    // Assign slot from the record's ID
    case 1:
        slot = id.slot;
        break;
    }

    switch (1)
    {
    // Assign the record ID to the record
    case 1:
        record->id = id;
        break;
    }

    switch (1)
    {
    // Calculate the offset size for the slot based on the record size
    case 1:
        offslot = getRecordSize(rel->schema);
        break;
    }

    switch (1)
    {
    // Retrieve table management data from the relation's management data
    case 1:
        temp = (RM_tableData_mgmtData *)rel->mgmtData;
        break;
    }

    switch (1)
    {
    // Retrieve buffer manager from table management data
    case 1:
        bm = temp->bm;
        break;
    }

    switch (1)
    {
    // Create a new page handle
    case 1:
        page = MAKE_PAGE_HANDLE();
        break;
    }

    // Pin the page
    rc = pinPageHelper(bm, page, pageNum);
    switch (rc)
    {
    case RC_OK:
        break;
    default:
        return rc;
    }

    // Copy data from the page to the record
    copyDataToRecord(record, page, slot, offslot);

    // Unpin the page after modifications
    rc = unpinPageHelper(bm, page, pageNum);
    switch (rc)
    {
    case RC_OK:
        break;
    default:
        return rc;
    }

    return RC_OK;
}

// Subfunction to validate input parameters
RC validateScanInput(RM_TableData *rel, RM_ScanHandle *scan, Expr *cond)
{
    switch ((rel == NULL || scan == NULL || cond == NULL) ? 1 : 0)
    {
    case 1:
        return RC_RM_UNKOWN_DATATYPE;
    case 0:
        break;
    }
    return RC_OK;
}

// Subfunction to initialize scan management data
void initializeScanData(RM_ScanData_mgmtData *ScanMgm, Expr *cond)
{
    switch (1)
    {
        // Initialize totalScan to 0
        case 1:
            ScanMgm->totalScan = 0;
            break;
    }

    switch (1)
    {
        // Assign the condition to ScanMgm
        case 1:
            ScanMgm->cond = cond;
            break;
    }

    switch (1)
    {
        // Set the current RID's page to 0
        case 1:
            ScanMgm->currentRID.page = 0;
            break;
    }

    switch (1)
    {
        // Set the current RID's slot to 0
        case 1:
            ScanMgm->currentRID.slot = 0;
            break;
    }
}


// Main startScan function
RC startScan(RM_TableData *rel, RM_ScanHandle *scan, Expr *cond)
{
    // Validate input parameters
    RC rc = validateScanInput(rel, scan, cond);
    switch (rc)
    {
    case RC_OK:
        break;
    default:
        return rc;
    }

    // Allocate and initialize scan management data
    RM_ScanData_mgmtData *ScanMgm = (RM_ScanData_mgmtData *)malloc(sizeof(RM_ScanData_mgmtData));
    initializeScanData(ScanMgm, cond);

    // Assign scan management data and relation to the scan handle
    scan->mgmtData = ScanMgm;
    scan->rel = rel;

    return RC_OK;
}

// Subfunction to validate input parameters
RC validateNextInput(RM_ScanHandle *scan, Record *record)
{
    switch ((scan == NULL || record == NULL) ? 1 : 0)
    {
    case 1:
        return RC_RM_UNKOWN_DATATYPE;
    case 0:
        break;
    }
    return RC_OK;
}

// Subfunction to check if all records have been scanned
RC checkIfNoMoreTuples(RM_ScanData_mgmtData *ScanMgm, RM_tableData_mgmtData *tableMgm)
{
    switch ((ScanMgm->totalScan == tableMgm->numRecords) ? 1 : 0)
    {
    case 1:
        return RC_RM_NO_MORE_TUPLES;
    case 0:
        break;
    }
    return RC_OK;
}

// Subfunction to fetch the current record
RC fetchCurrentRecord(RM_TableData *tableData, RM_ScanData_mgmtData *ScanMgm, Record *record)
{
    RC rc = getRecord(tableData, ScanMgm->currentRID, record);
    switch (rc)
    {
    case RC_OK:
        break;
    default:
        return rc;
    }
    return RC_OK;
}

// Subfunction to evaluate the condition for the current record
RC evaluateRecordCondition(Record *record, RM_TableData *tableData, RM_ScanData_mgmtData *ScanMgm, Value **res)
{
    RC rc = evalExpr(record, tableData->schema, ScanMgm->cond, res);
    switch (rc)
    {
    case RC_OK:
        break;
    default:
        return rc;
    }
    return RC_OK;
}

// Subfunction to increment to the next slot and page
void incrementRID(RM_ScanData_mgmtData *ScanMgm, RM_tableData_mgmtData *tableMgm)
{
    ScanMgm->currentRID.slot++;
    if (ScanMgm->currentRID.slot == tableMgm->numRecordsPerPage)
    {
        ScanMgm->currentRID.page++;
        ScanMgm->currentRID.slot = 0;
    }
    ScanMgm->totalScan++;
}

// Main next function
RC next(RM_ScanHandle *scan, Record *record)
{
    // Validate input parameters
    RC rc = validateNextInput(scan, record);
    switch (rc)
    {
    case RC_OK:
        break;
    default:
        return rc;
    }

    RM_ScanData_mgmtData *ScanMgm = (RM_ScanData_mgmtData *)scan->mgmtData;
    RM_TableData *tableData = scan->rel;
    RM_tableData_mgmtData *tableMgm = (RM_tableData_mgmtData *)tableData->mgmtData;

    Value *res = (Value *)malloc(sizeof(Value));
    res->v.boolV = FALSE;

    do
    {
        // Check if all records have been scanned
        rc = checkIfNoMoreTuples(ScanMgm, tableMgm);
        switch (rc)
        {
        case RC_OK:
            break;
        case RC_RM_NO_MORE_TUPLES:
            free(res);
            return RC_RM_NO_MORE_TUPLES;
        default:
            return rc;
        }

        // Fetch the current record
        rc = fetchCurrentRecord(tableData, ScanMgm, record);
        switch (rc)
        {
        case RC_OK:
            break;
        default:
            free(res);
            return rc;
        }

        // Evaluate the condition for the current record
        rc = evaluateRecordCondition(record, tableData, ScanMgm, &res);
        switch (rc)
        {
        case RC_OK:
            break;
        default:
            free(res);
            return rc;
        }

        // Move to the next slot or page if needed
        incrementRID(ScanMgm, tableMgm);

    } while (!res->v.boolV); // Continue until a matching record is found

    // Ensure scan management data is updated
    scan->mgmtData = ScanMgm;
    free(res);

    return RC_OK;
}

// Subfunction to validate scan input
RC validateScanHandle(RM_ScanHandle *scan)
{
    switch ((scan == NULL) ? 1 : 0)
    {
    case 1:
        return RC_RM_UNKOWN_DATATYPE;
    case 0:
        break;
    }
    return RC_OK;
}

// Subfunction to free scan management data
void freeScanMgmtData(RM_ScanHandle *scan)
{
    free(scan->mgmtData);
    scan->mgmtData = NULL;
}

// Main closeScan function
RC closeScan(RM_ScanHandle *scan)
{
    // Validate the scan handle
    RC rc = validateScanHandle(scan);
    switch (rc)
    {
    case RC_OK:
        break;
    default:
        return rc;
    }

    // Free scan management data
    freeScanMgmtData(scan);

    return RC_OK;
}

// dealing with schemas
// Subfunction to validate schema
RC validateSchema(Schema *schema)
{
    switch ((schema == NULL) ? 1 : 0)
    {
    case 1:
        return RC_RM_SCHEMA_NOT_FOUND;
    case 0:
        break;
    }
    return RC_OK;
}

// Subfunction to calculate the size of a single attribute
RC calculateAttributeSize(DataType dataType, int typeLength, int *size)
{
    switch (dataType)
    {
    case DT_INT:
        *size += sizeof(int);
        break;
    case DT_STRING:
        *size += typeLength + 1;
        break;
    case DT_FLOAT:
        *size += sizeof(float);
        break;
    case DT_BOOL:
        *size += sizeof(bool);
        break;
    default:
        return RC_RM_UNKOWN_DATATYPE;
    }
    return RC_OK;
}

// Main getRecordSize function
int getRecordSize(Schema *schema)
{
    // Validate the schema
    RC rc = validateSchema(schema);
    switch (rc)
    {
    case RC_OK:
        break;
    default:
        return rc;
    }

    int recordSize = 0;

    // Loop through each attribute and calculate its size
    for (int i = 0; i < schema->numAttr; i++)
    {
        rc = calculateAttributeSize(schema->dataTypes[i], schema->typeLength[i], &recordSize);
        switch (rc)
        {
        case RC_OK:
            break;
        default:
            return rc;
        }
    }

    return recordSize;
}

// Subfunction to allocate memory for Schema structure
RC allocateSchemaMemory(Schema **SCHEMA, int numAttr, int keySize)
{
    *SCHEMA = (Schema *)malloc(sizeof(Schema));
    switch ((*SCHEMA == NULL) ? 1 : 0)
    {
    case 1:
        return RC_MEMORY_ALLOCATION_ERROR;
    case 0:
        break;
    }

    (*SCHEMA)->numAttr = numAttr;
    (*SCHEMA)->keySize = keySize;

    (*SCHEMA)->attrNames = (char **)malloc(sizeof(char *) * numAttr);
    (*SCHEMA)->typeLength = (int *)malloc(sizeof(int) * numAttr);
    (*SCHEMA)->dataTypes = (DataType *)malloc(sizeof(DataType) * numAttr);
    (*SCHEMA)->keyAttrs = (int *)malloc(sizeof(int) * keySize);

    if ((*SCHEMA)->attrNames == NULL || (*SCHEMA)->typeLength == NULL ||
        (*SCHEMA)->dataTypes == NULL || (*SCHEMA)->keyAttrs == NULL)
    {
        return RC_MEMORY_ALLOCATION_ERROR;
    }

    return RC_OK;
}

// Subfunction to allocate memory for attribute names
RC allocateAttributeNames(Schema *SCHEMA, int numAttr)
{
    for (int i = 0; i < numAttr; i++)
    {
        SCHEMA->attrNames[i] = (char *)malloc(sizeof(char *));
        switch ((SCHEMA->attrNames[i] == NULL) ? 1 : 0)
        {
        case 1:
            return RC_MEMORY_ALLOCATION_ERROR;
        case 0:
            break;
        }
    }
    return RC_OK;
}

// Main mallocSchema function
static Schema *mallocSchema(int numAttr, int keySize)
{
    Schema *SCHEMA;

    // Allocate memory to Schema structure
    RC rc = allocateSchemaMemory(&SCHEMA, numAttr, keySize);
    switch (rc)
    {
    case RC_OK:
        break;
    default:
        return NULL; // Return NULL in case of memory allocation failure
    }

    // Allocate memory for attribute names
    rc = allocateAttributeNames(SCHEMA, numAttr);
    switch (rc)
    {
    case RC_OK:
        break;
    default:
        return NULL; // Return NULL in case of memory allocation failure
    }

    return SCHEMA;
}

// Subfunction to copy attribute names into the schema
RC copyAttributeNames(Schema *schema, char **attrNames, int numAttr)
{
    for (int i = 0; i < numAttr; i++)
    {
        strcpy(schema->attrNames[i], attrNames[i]);
        switch ((schema->attrNames[i] == NULL) ? 1 : 0)
        {
        case 1:
            return RC_MEMORY_ALLOCATION_ERROR;
        case 0:
            break;
        }
    }
    return RC_OK;
}

// Main createSchema function
Schema *createSchema(int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys)
{
    // Allocate memory for the schema using mallocSchema
    Schema *schema = mallocSchema(numAttr, keySize);
    switch ((schema == NULL) ? 1 : 0)
    {
    case 1:
        return NULL; // Memory allocation failed
    case 0:
        break;
    }

    // Set schema properties
    schema->numAttr = numAttr;
    schema->dataTypes = dataTypes;
    schema->typeLength = typeLength;
    schema->keySize = keySize;
    schema->keyAttrs = keys;

    // Copy attribute names into the schema
    RC rc = copyAttributeNames(schema, attrNames, numAttr);
    switch (rc)
    {
    case RC_OK:
        break;
    default:
        return NULL; // Memory allocation failed for attribute names
    }

    return schema;
}

// Subfunction to validate the schema before freeing
RC validateSchemaBeforeFree(Schema *schema)
{
    switch ((schema == NULL) ? 1 : 0)
    {
    case 1:
        return RC_RM_SCHEMA_NOT_FOUND;
    case 0:
        break;
    }
    return RC_OK;
}

// Subfunction to free the individual components of the schema
void freeSchemaComponents(Schema *schema)
{
    switch (1)
    {
        // Free the attribute names array and set it to NULL
        case 1:
            free(schema->attrNames);
            schema->attrNames = NULL;
            break;
    }

    switch (1)
    {
        // Free the data types array and set it to NULL
        case 1:
            free(schema->dataTypes);
            schema->dataTypes = NULL;
            break;
    }

    switch (1)
    {
        // Free the type length array and set it to NULL
        case 1:
            free(schema->typeLength);
            schema->typeLength = NULL;
            break;
    }

    switch (1)
    {
        // Free the key attributes array and set it to NULL
        case 1:
            free(schema->keyAttrs);
            schema->keyAttrs = NULL;
            break;
    }
}


// Main freeSchema function
RC freeSchema(Schema *schema)
{
    // Validate schema before proceeding with free operations
    RC rc = validateSchemaBeforeFree(schema);
    switch (rc)
    {
    case RC_OK:
        break;
    default:
        return rc;
    }

    // Free the individual components of the schema
    freeSchemaComponents(schema);

    // Free the schema structure itself
    free(schema);
    schema = NULL;

    return RC_OK;
}

// Subfunction to allocate memory for the Record
RC allocateRecordMemory(Record **record)
{
    *record = (Record *)malloc(sizeof(Record));
    switch ((*record == NULL) ? 1 : 0)
    {
    case 1:
        return RC_MEMORY_ALLOCATION_ERROR;
    case 0:
        break;
    }
    return RC_OK;
}

// Subfunction to allocate memory for the record data
RC allocateRecordData(Record *record, Schema *schema)
{
    record->data = (char *)malloc(getRecordSize(schema));
    switch ((record->data == NULL) ? 1 : 0)
    {
    case 1:
        return RC_MEMORY_ALLOCATION_ERROR;
    case 0:
        break;
    }
    return RC_OK;
}

// Main createRecord function
RC createRecord(Record **record, Schema *schema)
{
    // Validate schema
    RC rc = validateSchema(schema);
    switch (rc)
    {
    case RC_OK:
        break;
    default:
        return rc; // Return error if schema is invalid
    }

    // Allocate memory for the record
    rc = allocateRecordMemory(record);
    switch (rc)
    {
    case RC_OK:
        break;
    default:
        return rc; // Return error if memory allocation fails
    }

    // Allocate memory for the record data
    rc = allocateRecordData(*record, schema);
    switch (rc)
    {
    case RC_OK:
        break;
    default:
        return rc; // Return error if memory allocation for data fails
    }
    // Allocate space for the NULL bitmap
    int bitmapSize = (schema->numAttr + 7) / 8; // One bit per attribute
    (*record)->nullBitmap = (char *)malloc(bitmapSize);
    memset((*record)->nullBitmap, 0, bitmapSize);

    return RC_OK;
}

// Subfunction to validate the record before freeing
RC validateRecordBeforeFree(Record *record)
{
    switch ((record == NULL) ? 1 : 0)
    {
    case 1:
        return RC_RM_UNKOWN_DATATYPE;
    case 0:
        break;
    }
    return RC_OK;
}

// Subfunction to free the record's data
void freeRecordData(Record *record)
{
    free(record->data);
    record->data = NULL;
}

// Main freeRecord function
RC freeRecord(Record *record)
{
    // Validate the record before freeing
    RC rc = validateRecordBeforeFree(record);
    switch (rc)
    {
    case RC_OK:
        break;
    default:
        return rc; // Return error if the record is invalid
    }

    // Free the record's data
    freeRecordData(record);

    // Free the record structure itself
    free(record);
    record = NULL;

    return RC_OK;
}

// Subfunction to validate record and schema
RC validateRecordAndSchema(Record *record, Schema *schema)
{
    switch ((record == NULL || schema == NULL) ? 1 : 0)
    {
    case 1:
        return RC_RM_SCHEMA_NOT_FOUND;
    case 0:
        break;
    }
    return RC_OK;
}

// Subfunction to allocate memory for the value
RC allocateValue(Value **value)
{
    *value = (Value *)malloc(sizeof(Value));
    switch ((*value == NULL) ? 1 : 0)
    {
    case 1:
        return RC_MEMORY_ALLOCATION_ERROR;
    case 0:
        break;
    }
    return RC_OK;
}

// Subfunction to extract attribute value based on data type
RC extractAttributeValue(Schema *schema, int attrNum, char *recordData, Value *value)
{
    switch (schema->dataTypes[attrNum])
    {
    case DT_INT:
        memcpy(&(value->v.intV), recordData, sizeof(int));
        break;
    case DT_STRING:
        value->v.stringV = (char *)malloc(schema->typeLength[attrNum] + 1);
        if (value->v.stringV == NULL)
            return RC_MEMORY_ALLOCATION_ERROR;
        memcpy(value->v.stringV, recordData, schema->typeLength[attrNum] + 1);
        break;
    case DT_FLOAT:
        memcpy(&(value->v.floatV), recordData, sizeof(float));
        break;
    case DT_BOOL:
        memcpy(&(value->v.boolV), recordData, sizeof(bool));
        break;
    default:
        return RC_RM_UNKOWN_DATATYPE;
    }

    value->dt = schema->dataTypes[attrNum];
    return RC_OK;
}

// Main getAttr function
RC getAttr(Record *record, Schema *schema, int attrNum, Value **value)
{
    // Check NULL bitmap
    int byteIndex = attrNum / 8;
    int bitIndex = attrNum % 8;

    if (record->nullBitmap[byteIndex] & (1 << bitIndex))
    {
        MAKE_VALUE(*value, DT_NULL, 0); // Return a NULL value
        return RC_OK;
    }
    // Validate the record and schema
    RC rc = validateRecordAndSchema(record, schema);
    switch (rc)
    {
    case RC_OK:
        break;
    default:
        return rc; // Return error if record or schema is invalid
    }

    // Allocate memory for value
    rc = allocateValue(value);
    switch (rc)
    {
    case RC_OK:
        break;
    default:
        return rc; // Return error if memory allocation fails
    }

    // Calculate attribute offset
    int offattr = 0;
    rc = attrOffset(schema, attrNum, &offattr);
    switch (rc)
    {
    case RC_OK:
        break;
    default:
        return rc; // Return error if offset calculation fails
    }

    // Extract the attribute value
    char *recordData = record->data + offattr;
    rc = extractAttributeValue(schema, attrNum, recordData, *value);
    if (rc != RC_OK)
    {
        return rc;
    }

    return RC_OK;
}

// Subfunction to validate record, schema, and attribute number
RC validateRecordSchemaAndAttr(Record *record, Schema *schema, int attrNum)
{
    switch ((record == NULL || schema == NULL || attrNum < 0 || attrNum >= schema->numAttr) ? 1 : 0)
    {
    case 1:
        return RC_RM_SCHEMA_NOT_FOUND;
    case 0:
        break;
    }
    return RC_OK;
}

// Subfunction to calculate attribute offset
RC calculateAttrOffset(Schema *schema, int attrNum, int *offattr)
{
    RC rc = attrOffset(schema, attrNum, offattr);
    switch (rc)
    {
    case RC_OK:
        break;
    default:
        return rc;
    }
    return RC_OK;
}

// Subfunction to set the attribute value based on data type
RC setAttributeValue(Schema *schema, int attrNum, Value *value, char *recordData)
{
    switch (value->dt)
    {
    case DT_INT:
        memcpy(recordData, &(value->v.intV), sizeof(int));
        break;
    case DT_FLOAT:
        memcpy(recordData, &(value->v.floatV), sizeof(float));
        break;
    case DT_BOOL:
        memcpy(recordData, &(value->v.boolV), sizeof(bool));
        break;
    case DT_STRING:
        memcpy(recordData, value->v.stringV, schema->typeLength[attrNum] + 1);
        break;
    default:
        return RC_RM_UNKOWN_DATATYPE;
    }
    return RC_OK;
}

// Main setAttr function
RC setAttr(Record *record, Schema *schema, int attrNum, Value *value)
{
    // Validate record, schema, and attribute number
    RC rc = validateRecordSchemaAndAttr(record, schema, attrNum);
    switch (rc)
    {
    case RC_OK:
        break;
    default:
        return rc; // Return error if validation fails
    }

    // Calculate attribute offset
    int offattr = 0;
    rc = calculateAttrOffset(schema, attrNum, &offattr);
    switch (rc)
    {
    case RC_OK:
        break;
    default:
        return rc; // Return error if offset calculation fails
    }

    // Set the attribute value in the record
    char *recordData = record->data + offattr;
    rc = setAttributeValue(schema, attrNum, value, recordData);
    switch (rc)
    {
    case RC_OK:
        break;
    default:
        return rc; // Return error if setting the attribute fails
    }
    int byteIndex = attrNum / 8;
    int bitIndex = attrNum % 8;

    if (value->dt == DT_NULL)
    {
        record->nullBitmap[byteIndex] |= (1 << bitIndex); // Set the bit to indicate NULL
    }
    else
    {
        record->nullBitmap[byteIndex] &= ~(1 << bitIndex); // Clear the bit to indicate NOT NULL
        // Existing code for setting attribute value...
    }

    return RC_OK;
}
