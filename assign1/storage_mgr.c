#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <string.h>
#include "storage_mgr.h"

// Initialize the Storage Manager
extern void initStorageManager(void) {
    // No initialization needed for this assignment
}

// Create a new page file with an empty first page
extern RC createPageFile(char *fileName) {
    FILE *fileHandle = fopen(fileName, "wb");
    if (fileHandle == NULL) {
        return RC_FILE_NOT_FOUND;  // File creation failed
    }

    // Allocate and initialize an empty page
    SM_PageHandle emptyPage = (SM_PageHandle) calloc(PAGE_SIZE, sizeof(char));
    if (emptyPage == NULL) {
        fclose(fileHandle);
        return RC_WRITE_FAILED;  // Memory allocation failed
    }

    // Write the empty page to the file
    size_t written = fwrite(emptyPage, sizeof(char), PAGE_SIZE, fileHandle);
    free(emptyPage);
    fclose(fileHandle);

    return (written == PAGE_SIZE) ? RC_OK : RC_WRITE_FAILED;
}

// Open an existing page file and initialize file handle metadata
extern RC openPageFile(char *fileName, SM_FileHandle *fHandle) {
    FILE *file = fopen(fileName, "rb+");  // Open file for read/write
    if (file == NULL) {
        return RC_FILE_NOT_FOUND;
    }

    // Move to end to determine file size
    fseek(file, 0, SEEK_END);
    long fileSize = ftell(file);
    fseek(file, 0, SEEK_SET);  // Reset file pointer to beginning

    fHandle->fileName = fileName;
    fHandle->totalNumPages = fileSize / PAGE_SIZE;
    fHandle->curPagePos = 0;
    fHandle->mgmtInfo = file;  // Store FILE* pointer for future use

    return RC_OK;
}

// Close an open page file
extern RC closePageFile(SM_FileHandle *fHandle) {
    if (fHandle == NULL || fHandle->mgmtInfo == NULL) {
        return RC_FILE_HANDLE_NOT_INIT;
    }

    fclose((FILE*) fHandle->mgmtInfo);
    fHandle->mgmtInfo = NULL;
    return RC_OK;
}

// Delete a page file
extern RC destroyPageFile(char *fileName) {
    if (access(fileName, F_OK) != 0) {  // Check if file exists
        return RC_FILE_NOT_FOUND;
    }
    return (remove(fileName) == 0) ? RC_OK : RC_FILE_DELETE_FAILED;
}

// Read a specific block from a file
extern RC readBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
    if (fHandle == NULL || memPage == NULL || pageNum >= fHandle->totalNumPages || pageNum < 0) {
        return RC_READ_NON_EXISTING_PAGE;
    }

    FILE *file = (FILE*) fHandle->mgmtInfo;
    fseek(file, pageNum * PAGE_SIZE, SEEK_SET);
    
    size_t bytesRead = fread(memPage, sizeof(char), PAGE_SIZE, file);
    if (bytesRead < PAGE_SIZE) {
        return RC_READ_NON_EXISTING_PAGE;
    }

    fHandle->curPagePos = pageNum;
    return RC_OK;
}

// Return the current page position
extern int getBlockPos(SM_FileHandle *fHandle) {
    return fHandle->curPagePos;
}

// Read first block
extern RC readFirstBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    return readBlock(0, fHandle, memPage);
}

// Read previous block
extern RC readPreviousBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    if (fHandle->curPagePos == 0) {
        return RC_READ_NON_EXISTING_PAGE;
    }
    return readBlock(fHandle->curPagePos - 1, fHandle, memPage);
}

// Read current block
extern RC readCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    return readBlock(fHandle->curPagePos, fHandle, memPage);
}

// Read next block
extern RC readNextBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    if (fHandle->curPagePos >= fHandle->totalNumPages - 1) {
        return RC_READ_NON_EXISTING_PAGE;
    }
    return readBlock(fHandle->curPagePos + 1, fHandle, memPage);
}

// Read last block
extern RC readLastBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    return readBlock(fHandle->totalNumPages - 1, fHandle, memPage);
}

// ✅ Fixed `writeBlock()` to correctly return RC_WRITE_FAILED when writing beyond totalNumPages without ensureCapacity()
extern RC writeBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
    if (pageNum < 0 || pageNum >= fHandle->totalNumPages) {
        return RC_WRITE_FAILED;  // ✅ Fail instead of expanding
    }

    FILE *file = (FILE*) fHandle->mgmtInfo;
    fseek(file, pageNum * PAGE_SIZE, SEEK_SET);

    size_t bytesWritten = fwrite(memPage, sizeof(char), PAGE_SIZE, file);
    if (bytesWritten < PAGE_SIZE) {
        return RC_WRITE_FAILED;
    }

    fHandle->curPagePos = pageNum;
    return RC_OK;
}

// Write to the current block
extern RC writeCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    return writeBlock(fHandle->curPagePos, fHandle, memPage);
}

// Append an empty block to the file
extern RC appendEmptyBlock(SM_FileHandle *fHandle) {
    FILE *file = (FILE*) fHandle->mgmtInfo;
    
    SM_PageHandle emptyPage = (SM_PageHandle) calloc(PAGE_SIZE, sizeof(char));
    if (emptyPage == NULL) {
        return RC_WRITE_FAILED;
    }

    fseek(file, 0, SEEK_END);
    size_t written = fwrite(emptyPage, sizeof(char), PAGE_SIZE, file);
    free(emptyPage);

    if (written < PAGE_SIZE) {
        return RC_WRITE_FAILED;
    }

    fHandle->totalNumPages++;
    return RC_OK;
}

// Ensure the file has at least `numberOfPages` pages
extern RC ensureCapacity(int numberOfPages, SM_FileHandle *fHandle) {
    while (fHandle->totalNumPages < numberOfPages) {
        appendEmptyBlock(fHandle);
    }
    return RC_OK;
}
