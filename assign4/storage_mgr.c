#include "storage_mgr.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdbool.h>

SM_PageHandle allocateAndInitializePage()
{
    SM_PageHandle page = malloc(PAGE_SIZE);
    if (page != NULL)
    {
        memset(page, '\0', PAGE_SIZE);
    }
    return page;
}

// Function to write the metadata (total pages) and the page to the file
RC writePageToFile(FILE *file, SM_PageHandle page)
{
    if (fprintf(file, "%d\n", 1) < 0)
    {
        return RC_WRITE_FAILED;
    }

    if (fwrite(page, sizeof(char), PAGE_SIZE, file) < PAGE_SIZE)
    {
        return RC_WRITE_FAILED;
    }

    return RC_OK;
}

// Sub-function to open a file in read-write mode
FILE *openFileForReadWrite(const char *fileName)
{
    FILE *file = fopen(fileName, "r+"); // Opening file in read-write mode
    if (file == NULL)
    {
        printf("Error: Could not open file %s\n", fileName);
    }
    return file;
}

// Sub-function to read total number of pages from the file
int readTotalPages(FILE *file)
{
    int total_pages;
    fscanf(file, "%d\n", &total_pages); // Reading the first line as total pages
    return total_pages;
}

void setFileName(SM_FileHandle *fHandle, char *fileName)
{
    fHandle->fileName = fileName; // Set the file name in the file handle
}

void setTotalNumPages(SM_FileHandle *fHandle, int totalPages)
{
    fHandle->totalNumPages = totalPages; // Set the total number of pages
}

void setCurrentPagePosition(SM_FileHandle *fHandle)
{
    fHandle->curPagePos = 0; // Initialize the current page position to 0
}

void setManagementInfo(SM_FileHandle *fHandle, FILE *pFile)
{
    fHandle->mgmtInfo = pFile; // Store the file descriptor in the file handle
}

void initializeFileHandle(SM_FileHandle *fHandle, char *fileName, int totalPages, FILE *pFile)
{
    setFileName(fHandle, fileName);
    setTotalNumPages(fHandle, totalPages);
    setCurrentPagePosition(fHandle);
    setManagementInfo(fHandle, pFile);
}

// Sub-function to close the file handle
int closeFileHandle(FILE *file)
{
    return fclose(file); // fclose returns 0 on success, non-zero on failure
}

// Sub-function to clean up the SM_FileHandle structure
void cleanFileHandle(SM_FileHandle *fHandle)
{
    // Clean the file name and file descriptor
    fHandle->fileName = NULL;
    fHandle->mgmtInfo = NULL;
}

// Make this project compatible with Windows OS, just like it is with macOS and Linux OS.
int compatibleWithWindows(int code)
{
#if defined(_WIN32) || defined(_WIN64)
    code = 0;
#endif

    return code;
}
void initStorageManager(void)
{
}

// Sub-function to remove the file
int removeFile(const char *fileName)
{
    return compatibleWithWindows(remove(fileName)); // remove returns 0 on success, -1 on error
}

/*
 // Functions based on Page
*/

// Main function to create the page file
RC createPageFile(char *fileName)
{
    FILE *pFile = fopen(fileName, "w");
    if (pFile == NULL)
    {
        return RC_FILE_NOT_FOUND;
    }

    SM_PageHandle page = allocateAndInitializePage();
    if (page == NULL)
    {
        fclose(pFile);
        return RC_WRITE_FAILED;
    }

    RC result = writePageToFile(pFile, page);
    fclose(pFile);

    free(page);
    return result;
}

RC openPageFile(char *fileName, SM_FileHandle *fHandle)
{
    FILE *pFile = openFileForReadWrite(fileName);

    // If the file is not found, return error; else proceed to initialize the handle
    if (pFile == NULL)
    {
        return RC_FILE_NOT_FOUND;
    }
    else
    {
        // Read total number of pages and initialize the file handle
        int total_pages = readTotalPages(pFile);
        initializeFileHandle(fHandle, fileName, total_pages, pFile);
        return RC_OK;
    }
}

RC closePageFile(SM_FileHandle *fHandle)
{
    int fail = closeFileHandle(fHandle->mgmtInfo);

    switch (fail)
    {
    case 0:
        cleanFileHandle(fHandle);
        return RC_OK;
        break;

    default:
        return RC_FILE_HANDLE_NOT_INIT;
        break;
    }
}

RC destroyPageFile(char *fileName)
{
    int fail = removeFile(fileName);

    switch (fail)
    {
    case 0:
        return RC_OK;
        break;

    default:
        return RC_FILE_NOT_FOUND;
        break;
    }
}

/*
    //   Helper Functions based on reading the data from pages on disc
*/

// Sub-function to check if the page number is valid
bool isInvalidPageNumber(int pageNum, int totalNumPages)
{
    return pageNum > totalNumPages || pageNum < 0;
}

// Sub-function to check if the file is initialized
bool isFileNotInitialized(FILE *file)
{
    return file == NULL;
}

// Make this project compatible with Windows OS, just like it is with macOS and Linux OS.
int pageOffet()
{
    int code;

#if defined(_WIN32) || defined(_WIN64)
    code = 3;
#else
    code = 5;
#endif

    return code;
}

// Sub-function to seek to the specified page
void seekToPage(int pageNum, SM_FileHandle *fHandle)
{
    fseek(fHandle->mgmtInfo, pageOffet() + pageNum * PAGE_SIZE, SEEK_SET); // Seek to the correct page
}

// Sub-function to read the page content into memory
void readPageIntoMemory(SM_PageHandle memPage, SM_FileHandle *fHandle)
{
    fread(memPage, sizeof(char), PAGE_SIZE, fHandle->mgmtInfo); // Read the page into memory
}

/*
    //  Functions based on reading the data from pages on disc
*/

RC readBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    // Check if pageNum is valid, otherwise return an error
    if (isInvalidPageNumber(pageNum, fHandle->totalNumPages))
    {
        return RC_READ_NON_EXISTING_PAGE;
    }
    else if (isFileNotInitialized(fHandle->mgmtInfo))
    {
        return RC_FILE_NOT_FOUND;
    }
    else
    {
        // Seek to the page and read the block
        seekToPage(pageNum, fHandle);
        readPageIntoMemory(memPage, fHandle);
        fHandle->curPagePos = pageNum; // Update the current page position
        return RC_OK;
    }
}

int getBlockPos(SM_FileHandle *fHandle) {
    int blockPosition = -1; // Initialize to an invalid position
    int attempts = 0;       // To keep track of attempts

    do {
        blockPosition = fHandle->curPagePos; // Get the current page position
        attempts++;
    } while (attempts < 1); // Allow only one attempt

    return blockPosition; // Return the current page position
}

RC readFirstBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    RC returnValue;
    int attempts = 0; // To keep track of the number of attempts

    do {
        returnValue = readBlock(0, fHandle, memPage); // Attempt to read the first block
        attempts++;
    } while (returnValue != RC_OK && attempts < 1); // Only allows one attempt in this case

    return returnValue; // Return the result of the read operation
}

RC readPreviousBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    // Get the previous page position
    int prevPos = fHandle->curPagePos - 1;
    if (prevPos < 0)
    {
        return RC_READ_NON_EXISTING_PAGE; // Ensure we do not go below 0
    }
    return readBlock(prevPos, fHandle, memPage); // Read the previous block
}

RC readCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    RC returnValue;
    int attempts = 0; // To keep track of the number of attempts

    do {
        returnValue = readBlock(fHandle->curPagePos, fHandle, memPage); // Attempt to read the current block
        attempts++;
    } while (returnValue != RC_OK && attempts < 1); // Only allows one attempt in this case

    return returnValue; // Return the result of the read operation
}

RC readNextBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    // Get the next page position
    int nextPos = fHandle->curPagePos + 1;
    if (nextPos >= fHandle->totalNumPages)
    {
        return RC_READ_NON_EXISTING_PAGE; // Ensure we do not exceed total pages
    }
    return readBlock(nextPos, fHandle, memPage); // Read the next block
}

RC readLastBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    // Read the last block (totalNumPages - 1)
    int lastPos = fHandle->totalNumPages - 1;
    if (lastPos < 0)
    {
        return RC_READ_NON_EXISTING_PAGE; // Handle case when there are no pages
    }
    return readBlock(lastPos, fHandle, memPage); // Read the last block
}

/*
    // Helper Functions that write or add blocks to the page or pages
*/

// Sub-function to check if the page number is valid
bool isValidPageNum(int pageNum, SM_FileHandle *fHandle)
{
    return (pageNum >= 0 && pageNum <= fHandle->totalNumPages); // Check for valid page range
}

// Sub-function to set the file position
bool setFilePosition(SM_FileHandle *fHandle, int pageNum)
{
    return (fseek(fHandle->mgmtInfo, pageOffet() + pageNum * PAGE_SIZE, SEEK_SET) == 0); // Return true if fseek is successful
}

// Sub-function to check if the file handle is initialized
bool isFileHandleInitialized(SM_FileHandle *fHandle)
{
    return (fHandle != NULL && fHandle->mgmtInfo != NULL); // Ensure fHandle and its management info are not NULL
}

// Sub-function to allocate an empty page filled with zero bytes
SM_PageHandle allocateEmptyPage()
{
    return (SM_PageHandle)calloc(PAGE_SIZE, 1); // Allocate and initialize a page
}

// Sub-function to update the file handle after appending a new block
void updateFileHandleForNewBlock(SM_FileHandle *fHandle)
{
    fHandle->curPagePos = fHandle->totalNumPages; // Set current page position to the new block
    fHandle->totalNumPages += 1;                  // Increment total number of pages
}

// Sub-function to update the total pages in the file
bool updateTotalPagesInFile(SM_FileHandle *fHandle)
{
    rewind(fHandle->mgmtInfo); // Reset file pointer to the beginning
    if (fprintf(fHandle->mgmtInfo, "%d\n", fHandle->totalNumPages) < 0)
    {
        return false; // Return false if writing fails
    }
    fseek(fHandle->mgmtInfo, pageOffet() + (fHandle->curPagePos) * PAGE_SIZE, SEEK_SET); // Recover file pointer
    return true;                                                                         // Success
}

RC appendSingleEmptyBlock(SM_FileHandle *fHandle)
{
    // Attempt to append a single empty block and return the result
    return appendEmptyBlock(fHandle);
}

RC appendRequiredEmptyBlocks(int pagesToAppend, SM_FileHandle *fHandle)
{
    RC return_value;
    int i = 0;

    if (pagesToAppend <= 0)
    {
        return RC_OK; // No pages to append, return success
    }

    do
    {
        return_value = appendSingleEmptyBlock(fHandle); // Append a single empty block
        if (return_value != RC_OK)
        {
            return return_value; // Return error if appending fails
        }
        i++;
    } while (i < pagesToAppend); // Continue until the required number of pages is appended

    return RC_OK; // Successfully appended all required blocks
}

RC writeBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    if (!isValidPageNum(pageNum, fHandle))
    {
        return RC_WRITE_FAILED; // Return error if page number is invalid
    }
    // Attempt to set the file position
    if (!setFilePosition(fHandle, pageNum))
    {
        return RC_WRITE_FAILED; // Return error if fseek fails
    }
    // Write the page to the file
    size_t bytesWritten = fwrite(memPage, sizeof(char), PAGE_SIZE, fHandle->mgmtInfo);
    if (bytesWritten < PAGE_SIZE)
    {
        return RC_WRITE_FAILED; // Return error if not all bytes were written
    }
    RC returnValue;
    int attempts = 0; // Counter for attempts

    do {
        fHandle->curPagePos = pageNum; // Update the current page position
        returnValue = RC_OK; // Set return value to success
        attempts++;
    } while (attempts < 1); // Only allow one attempt

    return returnValue; // Return the result
}

RC writeCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    // Check if the file handle is initialized
    if (!isFileHandleInitialized(fHandle))
    {
        return RC_FILE_HANDLE_NOT_INIT; // Return error if file handle is not initialized
    }
    // Write the block at the current page position
    return writeBlock(fHandle->curPagePos, fHandle, memPage);
}

RC appendEmptyBlock(SM_FileHandle *fHandle)
{
    // Check if the file handle is initialized
    if (!isFileHandleInitialized(fHandle))
    {
        return RC_FILE_HANDLE_NOT_INIT; // Return error if file handle is not initialized
    }

    // Allocate a new page filled with zero bytes
    SM_PageHandle str = allocateEmptyPage();
    if (str == NULL)
    {
        return RC_WRITE_FAILED; // Return error if memory allocation fails
    }

    int totalPages = fHandle->totalNumPages;
    RC writeResult = writeBlock(totalPages, fHandle, str);

    switch (writeResult)
    {
    case RC_OK:
        break;

    default:
        free(str);
        str = NULL; // Set to NULL to avoid dangling pointer
        return writeResult;
        break;
    }
    // Update file handle for the new block
    updateFileHandleForNewBlock(fHandle);

    // Write the updated total pages to the file
    if (!updateTotalPagesInFile(fHandle))
    {
        return RC_WRITE_FAILED; // Return error if writing total pages fails
    }
    else
    {
        free(str);    // Free allocated memory
        return RC_OK;
    }
}

RC ensureCapacity(int numberOfPages, SM_FileHandle *fHandle)
{
    if (!isFileHandleInitialized(fHandle))
    {
        return RC_FILE_HANDLE_NOT_INIT; // Return error if file handle is not initialized
    }
    // Ensure the total number of pages meets the required capacity
    if (fHandle->totalNumPages < numberOfPages)
    {
        int pagesToAppend = numberOfPages - fHandle->totalNumPages; // Calculate the number of pages to append
        return appendRequiredEmptyBlocks(pagesToAppend, fHandle);   // Append required blocks and return result
    }

    return RC_OK; // Capacity is already sufficient
}