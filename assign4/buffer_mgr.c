// Including necessary headers
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include <stdio.h>
#include <string.h>
// #include <stdbool.h>
#include <stdlib.h>

/*
    // Helper functions for Pinning related functions
*/

// Displaying the current state of frames in the buffer
void printBufferPoolFrames(BufferManager *bufferManager)
{
    PageFrame *currentFrame = bufferManager->firstFrame;
    int frameCount = 0;

    printf("Frames in Buffer Pool:\n");
    do
    {
        printf("Frame %d:\n", frameCount);
        printf("  Page ID: %d\n", currentFrame->pageID);
        printf("  Is Modified: %s\n", currentFrame->isModified ? "true" : "false");
        printf("  Reference Count: %d\n", currentFrame->referenceCount);
        printf("  Next Frame: %p\n", (void *)currentFrame->nextFrame);
        printf("  Previous Frame: %p\n", (void *)currentFrame->prevFrame);
        currentFrame = currentFrame->nextFrame;
        frameCount++;
    } while (currentFrame != bufferManager->firstFrame);
}

// Incrementing the fix count of a frame
void incrementFixCount(PageFrame *frame)
{
    frame->referenceCount++;
}

// Checking if a page is already pinned in the buffer
PageFrame *alreadyPinned(BM_BufferPool *const bufferPool, const PageNumber pageID)
{
    BufferManager *bufferMgr = bufferPool->mgmtData;
    PageFrame *currentFrame = bufferMgr->firstFrame;

    if (currentFrame == NULL)
    {
        return NULL; // Return NULL if the buffer is empty
    }

    while (true)
    {
        if (currentFrame == NULL)
        {
            return NULL; // Return NULL if the buffer is empty
        }
        bool isFound = (currentFrame->pageID == pageID); // Check if the current frame matches the pageID

        switch (isFound)
        {
        case true:
            incrementFixCount(currentFrame);
            return currentFrame; // Page is already pinned
        case false:
            // Continue checking the next frame
            break;
        }

        currentFrame = currentFrame->nextFrame; // Move to the next frame

        // Check if we have looped back to the first frame
        if (currentFrame == bufferMgr->firstFrame)
        {
            return NULL; // Page not found
        }
    }
}

// Writing a dirty page back to disk
RC handleDirtyFrame(PageFrame *framePtr, SM_FileHandle *fileHandle, BufferManager *buffer)
{
    if (framePtr->isModified)
    {
        RC result = writeBlock(framePtr->pageID, fileHandle, framePtr->pageData);
        if (result != RC_OK)
        {
            return result;
        }
        else
        {
            framePtr->isModified = false; // Clearing the modified flag
            buffer->writeOperations++;    // Incrementing write operation count
        }
    }
    return RC_OK;
}

// Loading a page from disk and pinning it in memory
RC readAndPinPage(PageFrame *framePtr, PageNumber pageID, SM_FileHandle *fileHandle, BufferManager *buffer)
{
    RC result = readBlock(pageID, fileHandle, framePtr->pageData);
    if (result != RC_OK)
    {
        return result;
    }
    else
    {
        buffer->readOperations++;   // Incrementing read operation count
        framePtr->pageID = pageID;  // Setting the page ID
        framePtr->referenceCount++; // Increasing the reference count
    }
    return RC_OK;
}

void printFrameDetails(BufferManager *bufferManager)
{
    if (bufferManager == NULL || bufferManager->firstFrame == NULL)
    {
        printf("No frames available to print.\n");
        return;
    }

    PageFrame *currentFrame = bufferManager->firstFrame;
    PageFrame *startFrame = currentFrame; // Store the starting frame to detect a full loop
    int frameIndex = 0;

    printf("Frame Details:\n");
    do
    {
        printf("Frame %d:\n", frameIndex);
        printf("  Address: %p\n", (void *)currentFrame); // Print frame address
        printf("  Page ID: %d\n", currentFrame->pageID);
        printf("  Is Modified: %s\n", currentFrame->isModified ? "true" : "false");
        printf("  Reference Count: %d\n", currentFrame->referenceCount);
        printf("  Accessed: %s\n", currentFrame->accessed ? "true" : "false");
        printf("  Data: ");
        for (int i = 0; i < PAGE_SIZE; i++)
        {
            printf("%c", currentFrame->pageData[i]); // Print data in the frame
        }
        printf("\n\n");

        currentFrame = currentFrame->nextFrame; // Move to the next frame
        frameIndex++;
    } while (currentFrame != NULL && currentFrame != startFrame); // Check for full loop
}

// Function to write back the current frame if it's dirty
RC writeBackIfDirty(PageFrame *currentFrame, SM_FileHandle *fHandle, BufferManager *bufferManager)
{
    if (currentFrame->isModified)
    {
        RC result = writeBlock(currentFrame->pageID, fHandle, currentFrame->pageData);
        if (result != RC_OK)
        {
            return result; // Check for errors
        }
        currentFrame->isModified = false; // Reset the dirty flag
        bufferManager->writeOperations++; // Increment write operations
    }
    return RC_OK; // Return success
}

// Function to read the requested page into the current frame
RC readPageIntoFrame(PageFrame *currentFrame, int pageNum, SM_FileHandle *fHandle, BufferManager *bufferManager)
{
    // Attempt to read the block from the file
    RC result = readBlock(pageNum, fHandle, currentFrame->pageData);
    if (result == RC_READ_NON_EXISTING_PAGE)
    {
        return RC_READ_NON_EXISTING_PAGE; // Return specific error for non-existing page
    }
    else if (result != RC_OK)
    {
        return result; // Return any other error encountered
    }

    // If successful, increment the buffer manager's read operations
    bufferManager->readOperations++;

    // Update the current frame with the newly loaded page details
    currentFrame->pageID = pageNum;
    currentFrame->referenceCount++;  // Increment the reference count

    return RC_OK; // Return success
}


RC pinThisPage(BM_BufferPool *const bm, PageFrame *currentFrame, PageNumber pageNum)
{
    // Accessing the buffer manager
    BufferManager *bufferManager = bm->mgmtData;

    SM_FileHandle fHandle; // File handle for page operations
    RC result;

    // Open the page file
    result = openPageFile(bm->pageFile, &fHandle);
    if (result != RC_OK)
    {
        return result; // Return error if file opening fails
    }

    // Ensure the file has enough pages to accommodate the requested page
    result = ensureCapacity(pageNum, &fHandle);
    if (result != RC_OK)
    {
        closePageFile(&fHandle); // Close the file handle on error
        return result;
    }

    // Write the current frame back to disk if it's dirty
    result = writeBackIfDirty(currentFrame, &fHandle, bufferManager);
    if (result != RC_OK)
    {
        closePageFile(&fHandle); // Close the file handle on error
        return result;
    }

    // Read the requested page into the current frame
    result = readPageIntoFrame(currentFrame, pageNum, &fHandle, bufferManager);
    if (result == RC_READ_NON_EXISTING_PAGE)
    {
        closePageFile(&fHandle);
        return RC_READ_NON_EXISTING_PAGE; // Handle the specific error for non-existing page
    }
    else if (result != RC_OK)
    {
        closePageFile(&fHandle); // Close the file handle on error
        return result;           // Return other errors
    }

    // Update the page number for the current frame
    currentFrame->pageID = pageNum;

    // Close the file handle
    closePageFile(&fHandle);

    return RC_OK; // Return success if all operations are successful
}

/*
    // Helper functions based on Pinning Frame with different strategies like FIFO and LRU Stategies
*/

bool findAvailableFrame(BufferManager *buffer, PageFrame **framePtr)
{
    PageFrame *currentFrame = buffer->firstFrame;

    // Loop to find an available frame
    do
    {
        switch (currentFrame->referenceCount)
        {
        case 0:
            *framePtr = currentFrame; // Assign the found frame to framePtr
            return true;              // Found an available frame
        default:
            // Do nothing, will continue to the next frame
            break;
        }

        currentFrame = currentFrame->nextFrame; // Move to the next frame

        switch ((currentFrame == buffer->firstFrame)? 1 : 0)
        {
        case 1:
            *framePtr = NULL; // No available frame found
            return false;     // Indicate that no frame was found
        case 0:
            break; 
        default:
            break; // Continue looping if not back at the first frame
        }

    } while (true); // Infinite loop, will return from inside

    // This line is never reached, included for clarity
    *framePtr = NULL;
    return false; // Indicate that no frame was found
}

void updateFrameList(BufferManager *buffer, PageFrame *currentFrame)
{
    if (currentFrame == buffer->firstFrame)
    {
        buffer->firstFrame = currentFrame->nextFrame;
    }

    switch (1)
    {
    case 1:
        currentFrame->prevFrame->nextFrame = currentFrame->nextFrame;
        break;
    }

    switch (1)
    {
    case 1:
        currentFrame->nextFrame->prevFrame = currentFrame->prevFrame;
        break;
    }

    switch (1)
    {
    case 1:
        currentFrame->prevFrame = buffer->lastFrame;
        break;
    }

    switch (1)
    {
    case 1:
        buffer->lastFrame->nextFrame = currentFrame;
        break;
    }

    switch (1)
    {
    case 1:
        buffer->lastFrame = currentFrame;
        break;
    }

    switch (1)
    {
    case 1:
        currentFrame->nextFrame = buffer->firstFrame;
        break;
    }

    switch (1)
    {
    case 1:
        buffer->firstFrame->prevFrame = currentFrame;
        break;
    }
}

void moveToTail(BM_BufferPool *bufferPool, PageFrame *currentFrame)
{
    BufferManager *buffer = bufferPool->mgmtData;

    if (currentFrame == buffer->firstFrame)
    {
        buffer->firstFrame = currentFrame->nextFrame;
    }

    switch (1)
    {
    case 1:
        currentFrame->prevFrame->nextFrame = currentFrame->nextFrame;
        break;
    }

    switch (1)
    {
    case 1:
        currentFrame->nextFrame->prevFrame = currentFrame->prevFrame;
        break;
    }

    switch (1)
    {
    case 1:
        currentFrame->prevFrame = buffer->lastFrame;
        break;
    }

    switch (1)
    {
    case 1:
        buffer->lastFrame->nextFrame = currentFrame;
        break;
    }

    switch (1)
    {
    case 1:
        buffer->lastFrame = currentFrame;
        break;
    }

    switch (1)
    {
    case 1:
        currentFrame->nextFrame = buffer->firstFrame;
        break;
    }

    switch (1)
    {
    case 1:
        buffer->firstFrame->prevFrame = currentFrame;
        break;
    }
}

// Sub-function to find a frame to pin using the CLOCK algorithm
PageFrame *findFrameToPin(BufferManager *buffer)
{
    PageFrame *currentFrame = buffer->firstFrame;
    do
    {
        // Check if the frame is unpinned and can be replaced
        if (currentFrame->referenceCount == 0)
        {
            if (!currentFrame->accessed) // refbit is 0, ready to be replaced
            {
                return currentFrame; // Found a frame to pin
            }
            currentFrame->accessed = false; // Reset reference bit
        }

        // Move to the next frame in the buffer
        currentFrame = currentFrame->nextFrame;

    } while (currentFrame != buffer->firstFrame); // Loop until we've checked all frames

    return NULL; // No available frame found for replacement
}

void updateLinkedList(BufferManager *bufferManager, PageFrame *currentFrame)
{
    if (currentFrame == bufferManager->firstFrame)
    {
        bufferManager->firstFrame = currentFrame->nextFrame;
    }

    switch (1)
    {
    case 1:
        currentFrame->prevFrame->nextFrame = currentFrame->nextFrame;
        break;
    }

    switch (1)
    {
    case 1:
        currentFrame->nextFrame->prevFrame = currentFrame->prevFrame;
        break;
    }

    switch (1)
    {
    case 1:
        currentFrame->prevFrame = bufferManager->lastFrame;
        break;
    }

    switch (1)
    {
    case 1:
        bufferManager->lastFrame->nextFrame = currentFrame;
        break;
    }

    switch (1)
    {
    case 1:
        bufferManager->lastFrame = currentFrame;
        break;
    }

    switch (1)
    {
    case 1:
        currentFrame->nextFrame = bufferManager->firstFrame;
        break;
    }

    switch (1)
    {
    case 1:
        bufferManager->firstFrame->prevFrame = currentFrame;
        break;
    }
}

/*
    // Functions based on Pinning Frame with different strategies like FIFO and LRU Stategies
*/

RC pinFIFO(BM_BufferPool *const bufferPool, BM_PageHandle *const pageHandle, const PageNumber pageID, bool fromLRU)
{
    // Check if the page is already pinned
    if (!fromLRU && alreadyPinned(bufferPool, pageID))
    {
        return RC_OK;
    }
    else
    {
        BufferManager *bufferManager = bufferPool->mgmtData;
        PageFrame *availableFrame = NULL;                                      // Pointer to hold the available frame
        bool isAvailable = findAvailableFrame(bufferManager, &availableFrame); // Call with correct parameters

        switch (!isAvailable ? 1 : 0)
        {
        case 1:
            return RC_IM_NO_MORE_ENTRIES;
            break;

        default:
            break;
        }
        // Pin the page to the found frame
        RC result = pinThisPage(bufferPool, availableFrame, pageID);
        int val = (result != RC_OK) ? 1 : 0;
        switch (val)
        {
        case 1:
            return result;
            break; 

        default:
            break;
        }
        // Set page handle data
        pageHandle->pageNum = pageID;
        pageHandle->data = availableFrame->pageData;

        // Update the linked list to move the pinned frame to the tail
        updateLinkedList(bufferManager, availableFrame);

        return RC_OK;
    }
}

RC pinLRU(BM_BufferPool *const bufferPool, BM_PageHandle *const pageHandle,
          const PageNumber pageID)
{
    // Check if the page is already pinned
    PageFrame *currentFrame = alreadyPinned(bufferPool, pageID);

    if (currentFrame)
    {
        // If the page is already pinned, move it to the tail of the list
        moveToTail(bufferPool, currentFrame);
    }
    else
    {
        // If the page is not pinned, use FIFO to pin the page
        return pinFIFO(bufferPool, pageHandle, pageID, true);
    }

    // Update the page handle with the page number and data
    pageHandle->pageNum = pageID;
    pageHandle->data = currentFrame->pageData;

    return RC_OK; // Return success
}

RC pinCLOCK(BM_BufferPool *const bufferPool, BM_PageHandle *const pageHandle, const PageNumber pageNum)
{
    PageFrame *selectedFrame = alreadyPinned(bufferPool, pageNum);
    if (alreadyPinned(bufferPool, pageNum))
    {
        pageHandle->pageNum = pageNum;
        pageHandle->data = selectedFrame->pageData;
        return RC_OK;
    }

    BufferManager *buffer = bufferPool->mgmtData;
    selectedFrame = findFrameToPin(buffer);

    if (selectedFrame == NULL)
    {
        return RC_IM_NO_MORE_ENTRIES; // No available frame
    }

    RC result = pinThisPage(bufferPool, selectedFrame, pageNum);
    if (result != RC_OK)
    {
        return result; // Return the error from pinning
    }

    pageHandle->pageNum = pageNum;
    pageHandle->data = selectedFrame->pageData;


    return RC_OK;
}

// Function not Implemented, minimun is to have two stategies
RC pinLRUK(BM_BufferPool *const bm, BM_PageHandle *const page,
           const PageNumber pageNum)
{
    return RC_OK;
}

/*
    //Helper Functions for Mandatory functions required for the Assignment as defined in buffer_mgr.h
*/

// Helper function to create a new PageFrame
PageFrame *createPageFrame()
{
    PageFrame *newFrame = malloc(sizeof(PageFrame));
    if (newFrame != NULL)
    {
        newFrame->pageID = NO_PAGE;
        newFrame->isModified = false;
        newFrame->referenceCount = 0;
        memset(newFrame->pageData, '\0', PAGE_SIZE);
        newFrame->nextFrame = NULL; // Initialize nextFrame pointer
        newFrame->prevFrame = NULL; // Initialize prevFrame pointer
    }
    return newFrame; // Return the new page frame
}

// Helper function to create a new FrameStatistics
FrameStatistics *createFrameStatistics(PageFrame *frame)
{
    FrameStatistics *newStat = malloc(sizeof(FrameStatistics));
    if (newStat != NULL)
    {
        newStat->currentFrame = frame;
        newStat->nextStat = NULL; // Initialize nextStat pointer
    }
    return newStat; // Return the new frame statistics
}

// Helper function to link two PageFrames
void linkFrames(PageFrame *previous, PageFrame *next)
{
    previous->nextFrame = next; // Link the next frame
    next->prevFrame = previous; // Link the previous frame
}

// Helper function to add a new FrameStatistics to the list
void addFrameToList(FrameStatistics *head, FrameStatistics *newStat)
{
    while (head->nextStat != NULL)
    {
        head = head->nextStat; // Traverse to the end of the list
    }
    head->nextStat = newStat; // Add the new statistics to the end
}

void completeCircularLink(BufferManager *bufferManager)
{
    int firstFrameExists = (bufferManager->firstFrame != NULL) ? 1 : 0;
    switch (firstFrameExists)
    {
    case 1:
        bufferManager->lastFrame = bufferManager->firstFrame;

        int lastFrameExists = (bufferManager->lastFrame != NULL) ? 1 : 0;
        switch (lastFrameExists)
        {
        case 1:
            bufferManager->firstFrame->prevFrame = bufferManager->lastFrame;
            bufferManager->lastFrame->nextFrame = bufferManager->firstFrame;
            break;
        default:
            break;
        }
        break;
    default:
        break;
    }
}

// Helper function to free all PageFrames
void freePageFrames(PageFrame *frame)
{
    while (frame != NULL)
    {
        PageFrame *next = frame->nextFrame; // Keep track of next frame
        free(frame);                        // Free current frame
        frame = next;                       // Move to next frame
    }
}

// Helper function to write a page to disk
RC writePageToDisk(PageFrame *frame, SM_FileHandle *fileHandle)
{
    return writeBlock(frame->pageID, fileHandle, frame->pageData); // Write the current page's data to disk
}

// Helper function to free all PageFrames in the buffer manager
void freePageFramesShutdown(BufferManager *bufferManager)
{
    PageFrame *currentFrame = bufferManager->firstFrame;
    if (currentFrame == NULL)
    {
        return; // Nothing to free
    }

    PageFrame *nextFrame;
    do
    {
        nextFrame = currentFrame->nextFrame; // Keep track of the next frame
        free(currentFrame);                  // Free the current frame
        currentFrame = nextFrame;            // Move to the next frame
    } while (currentFrame != bufferManager->firstFrame);
}

// Helper function to flush dirty pages to disk
RC flushDirtyPagesToDisk(BufferManager *bufferManager, SM_FileHandle *fileHandle)
{
    if (bufferManager == NULL || fileHandle == NULL)
    {
        return RC_FILE_NOT_FOUND; // Ensure valid input
    }

    PageFrame *currentFrame = bufferManager->firstFrame; // Start from the first frame
    RC result;

    if (currentFrame == NULL)
    {
        return RC_FILE_NOT_FOUND; // No frames to process
    }

    do
    {
        // Check if the page is dirty
        if (currentFrame->isModified)
        {
            result = writePageToDisk(currentFrame, fileHandle);
            if (result != RC_OK)
            {
                return result; // Return error if writing a dirty page fails
            }
            currentFrame->isModified = false; // Mark page as clean after writing
            switch (1)
            {
            case 1:
                bufferManager->writeOperations++;
                break;
            default:
                break;
            }
        }

        currentFrame = currentFrame->nextFrame; // Move to the next page frame

    } while (currentFrame != bufferManager->firstFrame); // Continue until we return to the first frame

    return RC_OK; // Successfully flushed all dirty pages
}

// Helper function to find the page frame for a specific page number
PageFrame *findPageFrame(BufferManager *bufferManager, int pageNum)
{

    PageFrame *currentFrame = bufferManager->firstFrame;
    if (currentFrame == NULL) {
        return NULL;
    }


    // Iterate through the circular linked list to find the page frame
    do
    {

        // Check if the current frame's pageID matches the requested pageNum
        int condition = (currentFrame->pageID == pageNum) ? 1 : 0; // Convert to integer for switch

        switch (condition)
        {
        case 1:
            return currentFrame; // Return the page frame if found
        case 0:
            currentFrame = currentFrame->nextFrame; // Move to the next page frame
            break;
        default:
            // No default case is necessary in this scenario
            break;
        }
    } while (currentFrame != bufferManager->firstFrame);

    return NULL; // Return NULL if the page frame is not found
}


RC updateFixCount(PageFrame *currentFrame)
{
    switch (currentFrame->referenceCount)
    {
    case 0:
        return RC_READ_NON_EXISTING_PAGE; // Return error if fix count is already 0
    default:
        currentFrame->referenceCount--;
        switch (currentFrame->referenceCount)
        {
        case 0:
            currentFrame->accessed = false; // Reset accessed flag
            break;
        default:
            break; // No action needed for referenceCount > 0
        }
        return RC_OK; // Successfully updated the fix count
    }
}

RC writePageData(PageNumber pageNum, SM_FileHandle *fileHandle, char *data)
{
    return writeBlock(pageNum, fileHandle, data);
}

// Sub-function to allocate memory for frame contents
PageNumber *allocateFrameArray(int numFrames)
{
    PageNumber *frameArray = calloc(numFrames, sizeof(PageNumber));
    return frameArray; // Return allocated array
}

// Sub-function to retrieve the page ID from the frame statistics
void retrieveFrameContents(FrameStatistics *statHead, PageNumber *frameNumbers, int *count, int maxFrames)
{
    do
    {
        if (*count < maxFrames) // Check bounds
        {
            frameNumbers[*count] = statHead->currentFrame->pageID; // Store current page ID
            statHead = statHead->nextStat;                         // Move to the next statistic
            (*count)++;                                            // Increment count
        }
    } while (statHead != NULL && *count < maxFrames); // Continue until all frames are processed
}

// Sub-function to allocate memory for dirty flags
bool *allocateDirtyFlags(int numFrames)
{
    bool *dirtyFlags = calloc(numFrames, sizeof(bool));
    return dirtyFlags; // Return allocated array
}

// Sub-function to set the dirty flag for each frame
void retrieveDirtyFlags(FrameStatistics *statHead, bool *dirtyFlags, int maxFrames)
{
    int count = 0;
    do
    {
        if (count < maxFrames) // Check bounds
        {
            dirtyFlags[count] = statHead->currentFrame->isModified; // Set dirty flag
            statHead = statHead->nextStat;                          // Move to the next statistic
            count++;                                                // Increment count
        }
    } while (statHead != NULL && count < maxFrames); // Continue until all frames are processed
}

// Sub-function to allocate memory for fix counts
int *allocateFixCounts(int numFrames)
{
    int *fixCounts = calloc(numFrames, sizeof(int)); // Allocate memory for fix counts
    return fixCounts;                                // Return allocated array
}

// Sub-function to retrieve fix counts for each frame
void retrieveFixCounts(FrameStatistics *statHead, int *fixCounts, int maxFrames)
{
    int count = 0;
    do
    {
        if (count < maxFrames) // Ensure within bounds
        {
            fixCounts[count] = statHead->currentFrame->referenceCount; // Set fix count
            statHead = statHead->nextStat;                             // Move to the next statistic
            count++;                                                   // Increment count
        }
    } while (statHead != NULL && count < maxFrames); // Continue until all frames are processed
}

// Sub-function to get the number of I/O operations
int getIOOperations(BufferManager *bufferManager, bool isReadOperation)
{
    return isReadOperation ? bufferManager->readOperations : bufferManager->writeOperations;
}

// Function to initialize the BufferManager
void initializeBufferManager(BufferManager *bufferManager, int totalFrames, void *strategyData)
{
    switch (1)
    {
    case 1:
        switch (RC_OK)
        {
        case RC_OK:
            bufferManager->totalPageFrames = totalFrames;
            break;
        default:
            break;
        }

        switch (RC_OK)
        {
        case RC_OK:
            bufferManager->strategyInfo = strategyData;
            break;
        default:
            break;
        }

        switch (RC_OK)
        {
        case RC_OK:
            bufferManager->readOperations = 0;
            break;
        default:
            break;
        }

        switch (RC_OK)
        {
        case RC_OK:
            bufferManager->writeOperations = 0;
            break;
        default:
            break;
        }

        // return RC_OK;
    }
}

// Helper function to allocate and initialize a PageFrame
PageFrame *initializePageFrame()
{
    PageFrame *frame = malloc(sizeof(PageFrame));
    switch ((frame != NULL) ? 1 : 0)
    {
    case 1:
        break;
    case 0:
        return NULL; // Memory allocation failed
    default:
        return NULL;
    }
    frame->pageID = NO_PAGE;
    frame->referenceCount = 0;
    frame->isModified = false;
    memset(frame->pageData, '\0', PAGE_SIZE);
    return frame;
}

// Helper function to allocate and initialize FrameStatistics
FrameStatistics *initializeFrameStatistics(PageFrame *frame)
{
    FrameStatistics *stat = malloc(sizeof(FrameStatistics));
    switch ((stat != NULL) ? 1 : 0)
    {
    case 1:
        // Successfully allocated memory for stat
        break;
    case 0:
        return NULL; // Memory allocation failed
    default:
        return NULL;
    }
    stat->nextStat = NULL;
    stat->currentFrame = frame;
    return stat;
}

RC createFirstPageFrame(BufferManager *bufferManager, PageFrame **headFrame, FrameStatistics **headStat)
{
    // Allocate memory for the first page frame
    *headFrame = initializePageFrame();
    if (*headFrame == NULL)
    {
        free(bufferManager);    // Free buffer manager if allocation fails
        return RC_WRITE_FAILED; // Return error if page frame creation fails
    }

    // Allocate memory for the first frame statistics
    *headStat = initializeFrameStatistics(*headFrame);
    if (*headStat == NULL)
    {
        free(*headFrame); // Free the allocated page frame if statistics creation fails
        free(bufferManager);
        return RC_WRITE_FAILED;
    }

    // Assign headFrame and headStat to the bufferManager
    bufferManager->firstFrame = *headFrame;
    bufferManager->statsHead = *headStat;

    return RC_OK;
}

RC createAdditionalFrames(BufferManager *bufferManager, PageFrame **headFrame, FrameStatistics **headStat, int totalFrames)
{

    // Loop to create the additional frames and statistics
    PageFrame *newFrame;
    for (int i = 1; i < totalFrames; i++)
    {
        newFrame = malloc(sizeof(PageFrame));
        switch ((newFrame == NULL) ? 1 : 0)
        {
        case 1:
            freePageFrames(bufferManager->firstFrame); // Free existing frames on failure
            free(*headStat);                           // Free the statistics head
            free(bufferManager);                       // Free the buffer manager
            return RC_WRITE_FAILED;
            break;
        case 0:
            break;

        default:
            break;
        }

        // Initialize the new page frame
        newFrame->pageID = NO_PAGE;
        newFrame->isModified = false;
        newFrame->referenceCount = 0;
        memset(newFrame->pageData, '\0', PAGE_SIZE);

        // Allocate memory for a new frame statistics
        FrameStatistics *newStat;
        do
        {
            newStat = malloc(sizeof(FrameStatistics));
            if (newStat != NULL)
            {
                break;
            }
            break;
        } while (true);

        switch (1)
        {
        case 1:
            if (newStat == NULL)
            {
                switch (1)
                {
                case 1:
                    free(newFrame);
                    break;
                default:
                    break;
                }

                freePageFrames(bufferManager->firstFrame);
                free(*headStat);
                free(bufferManager);
                return RC_WRITE_FAILED;
            }
            break;
        default:
            break;
        }

        // Link the new statistics to the current frame
        newStat->currentFrame = newFrame;

        // Link the current frame and statistics with the new ones
        (*headStat)->nextStat = newStat;
        *headStat = newStat; // Move to the new statistics

        (*headFrame)->nextFrame = newFrame;
        newFrame->prevFrame = *headFrame;
        *headFrame = newFrame; // Move to the new frame
    }

    return RC_OK; // Return success
}

/*
    // Mandatory functions required for the Assignment as defined in buffer_mgr.h
*/

RC forceFlushPool(BM_BufferPool *const bufferPool)
{
    if (bufferPool == NULL || bufferPool->mgmtData == NULL)
    {
        return RC_FILE_NOT_FOUND; // Return error if not open
    }

    SM_FileHandle fileHandle;
    BufferManager *bufferManager = bufferPool->mgmtData;
    RC result = openPageFile(bufferPool->pageFile, &fileHandle);

    switch (result)
    {
    case RC_OK:
        result = flushDirtyPagesToDisk(bufferManager, &fileHandle);
        closePageFile(&fileHandle); // Ensure file is closed
        return result;

    default:
        return result; // Return error from openPageFile
    }
}

RC initBufferPool(BM_BufferPool *const bufferPool, const char *const fileName,
                  const int totalFrames, ReplacementStrategy strategy,
                  void *strategyData)
{
    // Check if the page file exists
    FILE *file = fopen(fileName, "r");
    if (file == NULL)
    {
        return RC_FILE_NOT_FOUND; // Return error if the file does not exist
    }
    fclose(file); // Close the file after checking

    // Error check for total number of frames
    if (totalFrames <= 0)
    {
        return RC_WRITE_FAILED; // Return error if number of frames is invalid
    }

    // Initialize buffer manager
    BufferManager *bufferManager = malloc(sizeof(BufferManager));
    switch ((bufferManager == NULL) ? 1 : 0)
    {
    case 1:
        return RC_WRITE_FAILED;
        break;
    case 0:
        break;

    default:
        break;
    }
    initializeBufferManager(bufferManager, totalFrames, strategyData);

    PageFrame *headFrame = NULL;
    FrameStatistics *headStat = NULL;

    // Create the first page frame and statistics
    RC result = createFirstPageFrame(bufferManager, &headFrame, &headStat);
    if (result != RC_OK)
    {
        return result;
    }

    // Create the additional frames and statistics
    result = createAdditionalFrames(bufferManager, &headFrame, &headStat, totalFrames);
    if (result != RC_OK)
    {
        return result;
    }

    // Complete the circular linking for the clock algorithm
    headFrame->nextFrame = bufferManager->firstFrame;

    switch (1)
    {
    case 1:
        bufferManager->firstFrame->prevFrame = headFrame;
        break;
    }

    switch (1)
    {
    case 1:
        bufferManager->lastFrame = headFrame;
        break;
    }

    // Initialize buffer pool
    switch (1)
    {
    case 1:
        switch (RC_OK)
        {
        case RC_OK:
            bufferPool->numPages = totalFrames;
            break;
        default:
            break;
        }

        switch (RC_OK)
        {
        case RC_OK:
            bufferPool->pageFile = (char *)fileName;
            break;
        default:
            break;
        }

        switch (RC_OK)
        {
        case RC_OK:
            bufferPool->strategy = strategy;
            break;
        default:
            break;
        }

        switch (RC_OK)
        {
        case RC_OK:
            bufferPool->mgmtData = bufferManager;
            break;
        default:
            break;
        }

        return RC_OK;
    }

    return RC_OK;
}

RC shutdownBufferPool(BM_BufferPool *const bufferPool)
{
    if (bufferPool == NULL || bufferPool->mgmtData == NULL)
    {
        return RC_FILE_NOT_FOUND; // Check if buffer pool is open
    }
    // Write dirty pages to disk
    RC result = forceFlushPool(bufferPool);
    if (result != RC_OK)
    {
        return result; // Return error if flushing fails
    }

    // Free up resources
    BufferManager *bufferManager = bufferPool->mgmtData;
    if (bufferManager == NULL)
    {
        return RC_OK; // Return if no buffer manager to free
    }

    // Free all page frames
    freePageFramesShutdown(bufferManager);

    do
    {
        // Free buffer manager itself
        switch ((bufferManager != NULL)? 1 : 0)
        {
        case 1:
            free(bufferManager);
            break;
        case 0:
            break;
        }

        // Reset buffer pool properties
        switch (1)
        {
        case 1:
            switch (RC_OK)
            {
            case RC_OK:
                bufferPool->numPages = 0;
                break;
            default:
                break;
            }

            switch (RC_OK)
            {
            case RC_OK:
                bufferPool->pageFile = NULL;
                break;
            default:
                break;
            }

            switch (RC_OK)
            {
            case RC_OK:
                bufferPool->mgmtData = NULL;
                break;
            default:
                break;
            }

            return RC_OK;
        }

        return RC_OK; // Successfully shutdown the buffer pool
    } while (true);
}

RC markDirty(BM_BufferPool *const bufferPool, BM_PageHandle *const pageHandle ,const PageNumber pageNum)
{
    BufferManager *bufferManager = bufferPool->mgmtData;
    PageFrame *currentFrame = bufferManager->firstFrame;

    do
    {
        switch ((bufferPool == NULL || bufferPool->mgmtData == NULL)? 1 : 0)
        {
        case 1:
            return RC_FILE_NOT_FOUND; // Buffer pool is not open
        case 0:
            bufferManager = bufferPool->mgmtData;
            break;
        }

        currentFrame = findPageFrame(bufferManager, pageNum);
        switch ((currentFrame == NULL)? 1 : 0)
        {
        case 1:
            return RC_READ_NON_EXISTING_PAGE; // Page does not exist
        case 0:
            currentFrame->isModified = true; // Mark the page as dirty
            break;
        }

        return RC_OK; // Successfully marked the page as dirty
    } while (true);
}

RC unpinPage(BM_BufferPool *const bufferPool, BM_PageHandle *const pageHandle, const PageNumber pageNum)
{
    // Check if the buffer pool is initialized
    if (bufferPool == NULL || bufferPool->mgmtData == NULL)
    {
        return RC_FILE_NOT_FOUND; // Buffer pool not open
    };

    // Check if the page handle is valid
    if (pageHandle == NULL)
    {
        return RC_FILE_NOT_FOUND; // Invalid page handle
    }
    // Get the buffer manager from the buffer pool
    BufferManager *bufferManager = bufferPool->mgmtData;

    // Find the corresponding page frame for the given page number
    PageFrame *currentFrame = findPageFrame(bufferManager, pageNum);

    if (currentFrame == NULL)
    {
        return RC_READ_NON_EXISTING_PAGE; // Return error if page does not exist
    }

    // Decrement the fix count and update the reference bit if needed
    return updateFixCount(currentFrame);
}

// Helper function to check if a page is in the buffer pool
bool isPageInBuffer(BufferManager *bufferManager, PageNumber pageID)
{
    PageFrame *currentFrame = bufferManager->firstFrame;

    // Iterate through frames to check for the page
    do
    {
        if (currentFrame->pageID == pageID && currentFrame->referenceCount > 0)
        {
            return true; // Page is found and pinned
        }
        currentFrame = currentFrame->nextFrame;
    } while (currentFrame != bufferManager->firstFrame);

    return false; // Page not found in the buffer
}

RC forcePage(BM_BufferPool *const bufferPool, BM_PageHandle *const pageHandle,const PageNumber pageNum)
{
    do
    {
        // Check if buffer pool is open
        if (bufferPool == NULL || bufferPool->mgmtData == NULL)
        {
            return RC_FILE_NOT_FOUND; // Return error if buffer pool is not open
        }

        BufferManager *bufferManager = bufferPool->mgmtData;
        SM_FileHandle fileHandle;
        RC result = openPageFile(bufferPool->pageFile, &fileHandle); // Open the page file

        switch (result)
        {
        case RC_OK:
            break; // File opened successfully
        default:
            return result; // Return error if opening the file fails
        }

        // Write the page data to disk
        result = writePageData(pageNum, &fileHandle, pageHandle->data);

        switch (result)
        {
        case RC_OK:
            bufferManager->writeOperations++; // Increment write operations count
            closePageFile(&fileHandle);
            return RC_OK; // Return success
        default:
            closePageFile(&fileHandle);
            return result; // Return error if writing fails
        }

    } while (true);
}

RC pinPage(BM_BufferPool *const bufferPool, BM_PageHandle *const pageHandle, const PageNumber pageNum)
{
    if (bufferPool == NULL || bufferPool->mgmtData == NULL)
    {
        return RC_FILE_NOT_FOUND; // Buffer pool not open
    }

    if (pageNum < 0)
    {
        return RC_IM_KEY_NOT_FOUND;
    }

    switch (bufferPool->strategy)
    {
    case RS_LRU_K:
    {
        return pinLRUK(bufferPool, pageHandle, pageNum);
    }

    case RS_FIFO:
    {
        RC result;
        bool success = false;

        do
        {
            result = pinFIFO(bufferPool, pageHandle, pageNum, false);

            switch (result)
            {
            case RC_OK:
                success = true;
                break;

            default:
                return result;
            }
        } while (!success);

        return RC_OK;
    }

    case RS_CLOCK:
    {
        RC result;
        bool success = false;

        do
        {
            result = pinCLOCK(bufferPool, pageHandle, pageNum);

            switch (result)
            {
            case RC_OK:
                success = true;
                break;
            default:
                return result;
            }
        } while (!success);

        return RC_OK;
    }

    case RS_LRU:
    {
        RC result;
        bool success = false;

        do
        {
            result = pinLRU(bufferPool, pageHandle, pageNum);

            switch (result)
            {
            case RC_OK:
                success = true;
                break;
            default:
                return result;
            }
        } while (!success);

        return RC_OK;
    }

    default:
    {
        return RC_IM_KEY_NOT_FOUND;
    }
    }
}

// Main function to get frame contents
PageNumber *getFrameContents(BM_BufferPool *const bufferPool)
{
    PageNumber *frameNumbers = allocateFrameArray(bufferPool->numPages); // Allocate memory for frame contents
    if (frameNumbers == NULL)                                            // Check for successful allocation
        return NULL;

    BufferManager *bufferManager = bufferPool->mgmtData;  // Access buffer manager
    FrameStatistics *statHead = bufferManager->statsHead; // Start from the head of the statistics list
    int count = 0;

    retrieveFrameContents(statHead, frameNumbers, &count, bufferPool->numPages); // Retrieve frame contents

    return frameNumbers; // Return the array of frame contents
}

// Main function to get dirty flags
bool *getDirtyFlags(BM_BufferPool *const bm)
{
    bool *dirtyFlags;
    BufferManager *bufferManager = bm->mgmtData;
    FrameStatistics *statHead = bufferManager->statsHead;

    do
    {
        dirtyFlags = allocateDirtyFlags(bm->numPages); // Attempt to allocate dirty flags

        switch ((dirtyFlags != NULL)? 1 : 0)
        {
        case 1:
            retrieveDirtyFlags(statHead, dirtyFlags, bm->numPages); // Retrieve dirty flags
            return dirtyFlags;                                      // Return the array of dirty flags
        case 0:
            return NULL; // Return NULL if allocation failed
        }
    } while (false); // We only need to attempt once

    // This line is unreachable but added to satisfy all code paths
    return NULL;
}

// Main function to get fix counts
int *getFixCounts(BM_BufferPool *const bufferPool)
{
    int *fixCounts = allocateFixCounts(bufferPool->numPages); // Allocate memory for fix counts
    if (fixCounts == NULL)                                    // Check for successful allocation
        return NULL;

    BufferManager *bufferManager = bufferPool->mgmtData;  // Access buffer manager
    FrameStatistics *statHead = bufferManager->statsHead; // Start from the head of the statistics list

    retrieveFixCounts(statHead, fixCounts, bufferPool->numPages); // Retrieve fix counts

    return fixCounts; // Return the array of fix counts
}

int getNumReadIO(BM_BufferPool *const bufferPool)
{
    BufferManager *bufferManager = bufferPool->mgmtData;
    switch (1)
    {
    case 1:
        return getIOOperations(bufferManager, true); // Retrieve read I/O count
    default:
        return 0; // In case of unexpected scenarios
    }
}

int getNumWriteIO(BM_BufferPool *const bufferPool)
{
    BufferManager *bufferManager = bufferPool->mgmtData; // Access buffer manager
    int writeIOCount;                                    // Variable to hold write I/O count

    do
    {
        writeIOCount = getIOOperations(bufferManager, false); // Retrieve write I/O count

        switch (writeIOCount)
        {
        case 0:
            return 0; // Return 0 if no write operations
        default:
            return writeIOCount; // Return the actual write I/O count
        }
    } while (false); // Loop will only execute once

    // This line is unreachable but added to satisfy all code paths
    return 0;
}
