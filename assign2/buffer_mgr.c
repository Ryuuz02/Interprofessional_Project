#include <stdio.h>
#include <stdlib.h>
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include <math.h>
#include <limits.h>
#include "dberror.h"


int bufferSize = 0;
int rearIndex = 0;
int writeCount = 0;
int hit = 0;
int clockPointer = 0;
int lfuPtr = 0;

typedef struct Page {
    SM_PageHandle data;
    PageNumber pageNum;
    int dirtyBit;
    int fixCount;
    int hitNum;
    int refNum;
} PageFrame;

extern void FIFO(BM_BufferPool *const bp, PageFrame *tgtPg) {
    PageFrame *frameArray = (PageFrame *)bp->mgmtData;
    int replaced = 0;
    int frameIndex = rearIndex % bufferSize;  // Wrap around buffer using modular arithmetic
    int checks = 0; // Track attempts to prevent infinite loop scenarios

    // Keep checking until we find a valid page to replace or exhaust all buffer slots
    while (replaced == 0 && checks < bufferSize) {
        // Evaluate if the current frame is eligible for replacement
        int canSwap = (frameArray[frameIndex].fixCount == 0) ? 1 : 0;
        int mustWriteBack = ((frameArray[frameIndex].dirtyBit == 1) && canSwap) ? 1 : 0;

        // Write dirty pages to disk before swapping
        if (mustWriteBack!=0) {
            SM_FileHandle fileHandle;
            RC status = openPageFile(bp->pageFile, &fileHandle);
            if (status == RC_OK) {
                RC writeStatus = writeBlock(frameArray[frameIndex].pageNum, &fileHandle, frameArray[frameIndex].data);
                if (writeStatus == RC_OK) {
                    writeCount=writeCount+1;  // Track number of writes
                }
            }
        }

        // If the frame is not being used, proceed with replacement
        if (canSwap != 0) { 
            SM_PageHandle tempData = tgtPg->data;
            PageNumber tempPage = tgtPg->pageNum;
            int tempDirty = tgtPg->dirtyBit;
            int tempFix = tgtPg->fixCount;
            frameArray[frameIndex].data = tempData;
            frameArray[frameIndex].pageNum = tempPage;
            frameArray[frameIndex].dirtyBit = tempDirty;
            frameArray[frameIndex].fixCount = tempFix;
            replaced = 1;
        }
        

        frameIndex = (frameIndex + 1) % bufferSize;
        checks=checks+1;  // Increment attempts tracker
    }
}
extern void LFU(BM_BufferPool *const bp, PageFrame *tgtPg) {
    PageFrame *frameArray = (PageFrame *)bp->mgmtData;  
    int checkIdx;
    int currentIdx;
    int minUsedIdx;
    int minFrequency;

    // Set initial values for the least frequently used search
    minFrequency = INT_MAX; 
    minUsedIdx = lfuPtr;  
    currentIdx = minUsedIdx;

    // Scan buffer to find an unpinned page
    for (int attempts = 0; attempts < bufferSize; attempts++) {  
        checkIdx = (currentIdx + attempts) % bufferSize;  
        if (frameArray[checkIdx].fixCount == 0) {  
            minUsedIdx = checkIdx;  
            minFrequency = frameArray[checkIdx].refNum;  
            break;  
        }  
    }  

    // Locate the least frequently accessed frame
    int cycles = 0;
    for (currentIdx = (minUsedIdx + 1) % bufferSize; cycles < bufferSize; cycles++) {  
        if (frameArray[currentIdx].fixCount == 0) {
            if (frameArray[currentIdx].refNum < minFrequency) {
                minUsedIdx = currentIdx;
                minFrequency = frameArray[currentIdx].refNum;
            }
        }
        currentIdx = (currentIdx + 1) % bufferSize;
    }

    // If the frame is dirty, write it to disk before replacing
    if (frameArray[minUsedIdx].dirtyBit) {  
        SM_FileHandle fileHandler;  
        if (openPageFile(bp->pageFile, &fileHandler) == RC_OK) {  
            RC status = writeBlock(frameArray[minUsedIdx].pageNum, &fileHandler, frameArray[minUsedIdx].data);
            if (status == RC_OK) {
                writeCount++;  // Track disk writes
            }
        }  
    }  

    // Declare targetFrame before using it
    PageFrame *targetFrame = &frameArray[minUsedIdx];

    // Replace the least frequently used frame with the new page
    targetFrame->data = tgtPg->data;
    targetFrame->pageNum = tgtPg->pageNum;
    targetFrame->dirtyBit = tgtPg->dirtyBit;
    targetFrame->fixCount = tgtPg->fixCount;

    // Move LFU pointer ahead for future replacements
    lfuPtr = (minUsedIdx + 1) % bufferSize;
}


extern void LRU(BM_BufferPool *const bufferPool, PageFrame *targetPage) {

    int scanIndex = 0;
    int lruFrameIdx = -1;
    int lowestHits = INT_MAX;
    PageFrame *frameSet = (PageFrame *)bufferPool->mgmtData;
    // Iterate over frames to find the least recently used page
    for (; scanIndex < bufferSize; scanIndex++) {
        if (frameSet[scanIndex].fixCount == 0) { 
            if (frameSet[scanIndex].hitNum < lowestHits) { 
                lruFrameIdx = scanIndex;
                lowestHits = frameSet[scanIndex].hitNum;
            }
        }
    }

    // Ensure that a valid LRU page was found
    if (lruFrameIdx != -1) {
        PageFrame *selectedFrame = &frameSet[lruFrameIdx];

        // Write back to disk if the selected page is marked dirty
        if (selectedFrame->dirtyBit) {
            SM_FileHandle diskFile;
            RC fileStatus = openPageFile(bufferPool->pageFile, &diskFile);
            
            if (fileStatus == RC_OK) {
                RC writeStatus = writeBlock(selectedFrame->pageNum, &diskFile, selectedFrame->data);
                if (writeStatus == RC_OK) {
                    writeCount++;  // Increment the number of disk writes
                }
            }
        }

        // Assign the new page data to the least recently used frame
        selectedFrame->data = targetPage->data;
        selectedFrame->pageNum = targetPage->pageNum;
        selectedFrame->dirtyBit = targetPage->dirtyBit;
        selectedFrame->fixCount = targetPage->fixCount;
        selectedFrame->hitNum = hit;  // Update the latest access count
    }
}

extern void CLOCK(BM_BufferPool *const bp, PageFrame *tgtPg) {
    PageFrame *bufferFrames = (PageFrame *)bp->mgmtData;
    int isReplaced = 0;

    // Loop until a suitable page is found and replaced
    for (; !isReplaced; clockPointer = (clockPointer + 1) % bufferSize) {
        clockPointer %= bufferSize; // Ensure pointer stays within buffer range

        // Fetch current frame reference
        PageFrame *currentFrame = &bufferFrames[clockPointer];

        // Check if the frame is available for replacement
        int isFrameAvailable = (currentFrame->fixCount == 0);
        int requiresWrite = (currentFrame->dirtyBit != 0) && isFrameAvailable;

        // Handle writing back to disk before replacing
        if (requiresWrite) {
            SM_FileHandle fileHandler;
            RC fileOpenResult = openPageFile(bp->pageFile, &fileHandler);
            if (fileOpenResult == RC_OK) {
                RC writeResult = writeBlock(currentFrame->pageNum, &fileHandler, currentFrame->data);
                if (writeResult == RC_OK) {
                    writeCount++;  // Update write counter
                }
            }
        }

        // Perform replacement if the frame is eligible
        if (isFrameAvailable) {
            currentFrame->data = tgtPg->data;
            currentFrame->pageNum = tgtPg->pageNum;
            currentFrame->dirtyBit = tgtPg->dirtyBit;
            currentFrame->fixCount = tgtPg->fixCount;
            currentFrame->hitNum = 1;  // Set as recently used
            isReplaced = 1;  // Exit loop after replacement
        } else {
            // If frame is in use, reset hitNum instead
            currentFrame->hitNum = 0;
        }
    }
}

extern RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName,
    const int numPages, ReplacementStrategy strategy,
    void *stratData) {
// Assigning buffer pool metadata
bm->pageFile = (char *)pageFileName;
bm->numPages = numPages;
bm->strategy = strategy;

// Dynamically allocate memory for page frames
void *allocatedMemory = malloc(sizeof(PageFrame) * numPages);
PageFrame *frameBuffer = (PageFrame *)allocatedMemory;

// Ensure allocation was successful
if (allocatedMemory == NULL) {
return RC_OK;  // Handle allocation failure appropriately
}

// Setting buffer size
bufferSize = numPages;

// Iterating through the buffer pool to initialize each frame
for (int idx = 0; idx < bufferSize; idx++) {
PageFrame *frame = &frameBuffer[idx];  
frame->data = NULL;
frame->pageNum = -1;
frame->dirtyBit = 0;
frame->fixCount = 0;
frame->hitNum = 0;
frame->refNum = 0;
}

// Linking allocated buffer memory to the buffer pool
bm->mgmtData = frameBuffer;

// Resetting global tracking variables
lfuPtr = 0;
clockPointer = 0;
writeCount = 0;

return RC_OK;  // Successful initialization
}

extern RC shutdownBufferPool(BM_BufferPool *const bufferInstance) {
    // Retrieve active pages from the buffer manager
    PageFrame *activeFrames = (PageFrame *)bufferInstance->mgmtData;

    // Commit any pending modifications before deallocating memory
    forceFlushPool(bufferInstance);

    // Flag to determine if shutdown can proceed
    bool canShutdown = true;
    
    // Traverse the buffer pool to verify no pages are still pinned
    for (int frameIndex = 0; frameIndex < bufferInstance->numPages; frameIndex++) {
        if (activeFrames[frameIndex].fixCount > 0) {
            canShutdown = false;
            break;  // If pinned pages exist, abort shutdown
        }
    }

    // Proceed with cleanup only if shutdown is allowed
    if (canShutdown) {
        free(activeFrames);  // Release memory allocated for buffer pages
        bufferInstance->mgmtData = NULL;  // Clear reference to deallocated memory
        return RC_OK;  // Successfully closed the buffer pool
    }
    
    return RC_PINNED_PAGES_IN_BUFFER;  // Indicate failure due to pinned pages
}


extern RC forceFlushPool(BM_BufferPool *const bufferPool) {
    // Extract the buffer page list from memory
    PageFrame *frameList = (PageFrame *)bufferPool->mgmtData;
    
    // Iterate over buffer frames using a for-loop instead of while-loop
    for (int frameIdx = 0; frameIdx < bufferPool->numPages; frameIdx++) {  
        PageFrame *activeFrame = &frameList[frameIdx];  // Point to the frame being processed
        
        // Determine if the page needs flushing (dirty and not in use)
        int isFlushable = (activeFrame->fixCount == 0) && (activeFrame->dirtyBit == 1);
        
        if (isFlushable) {  // If eligible, proceed with flushing
            SM_FileHandle fileDescriptor;
            
            // Open the page file and perform the write operation
            RC fileOpenStatus = openPageFile(bufferPool->pageFile, &fileDescriptor);
            if (fileOpenStatus == RC_OK) {
                RC blockWriteStatus = writeBlock(activeFrame->pageNum, &fileDescriptor, activeFrame->data);
                
                if (blockWriteStatus == RC_OK) {
                    activeFrame->dirtyBit = 0;  // Reset dirty flag post-write
                    writeCount++;  // Increment the write-tracking counter
                }
            }
        }
    }

    return RC_OK;  // Indicate completion of the flushing process
}

extern RC markDirty(BM_BufferPool *const bufferPool, BM_PageHandle *const targetPage) {
    // Fetch frame array from the buffer pool
    PageFrame *pageBlocks = (PageFrame *)bufferPool->mgmtData;

    // Use a for-loop instead of while-loop to traverse the buffer
    for (int currentIndex = 0; currentIndex < bufferPool->numPages; currentIndex++) {
        // Determine if the current frame matches the target page
        int isMatchingPage = (pageBlocks[currentIndex].pageNum == targetPage->pageNum);
        
        if (isMatchingPage) {  // If a match is found, mark it dirty
            pageBlocks[currentIndex].dirtyBit = 1;
            return RC_OK;  // Return success
        }
    }

    return RC_ERROR;  // If no match is found, return an error code
}

extern RC unpinPage(BM_BufferPool *const bufferPool, BM_PageHandle *const targetPage) {
    // Retrieve the page frame array from the buffer manager
    PageFrame *pageFrames = (PageFrame *)bufferPool->mgmtData;

    // Use a different looping method to iterate over buffer frames
    for (int framePos = 0; framePos < bufferSize; framePos++) {
        // Check if the current frame contains the specified page
        if (pageFrames[framePos].pageNum == targetPage->pageNum) {
            // Reduce fixCount only if it's greater than zero
            if (pageFrames[framePos].fixCount > 0) {
                pageFrames[framePos].fixCount--;
            }
            return RC_OK; // Exit after successfully unpinning the page
        }
    }
    
    return RC_OK; // If the page is not found, still return success
}

extern RC forcePage(BM_BufferPool *const bufferPool, BM_PageHandle *const targetPage) {
    // Retrieve frame data from the buffer manager
    PageFrame *bufferFrames = (PageFrame *)bufferPool->mgmtData;
    SM_FileHandle fileDescriptor;
    int frameIndex; // Declare iterator outside the loop

    // Attempt to open the file only once to minimize redundant calls
    if (openPageFile(bufferPool->pageFile, &fileDescriptor) != RC_OK) {
        return RC_ERROR; // Exit early if file opening fails
    }

    // Scan through the buffer frames to locate the target page
    for (frameIndex = 0; frameIndex < bufferSize; frameIndex++) {
        if (bufferFrames[frameIndex].pageNum == targetPage->pageNum) {
            // Write the located page to disk
            if (writeBlock(bufferFrames[frameIndex].pageNum, &fileDescriptor, bufferFrames[frameIndex].data) == RC_OK) {
                bufferFrames[frameIndex].dirtyBit = 0;  // Reset dirty flag
                writeCount++;  // Increment the count for successful write operations
            }
            return RC_OK;  // Exit early upon writing the target page
        }
    }
    
    return RC_OK;  // Return success even if no match is found
}

extern RC pinPage(BM_BufferPool *const bm, BM_PageHandle *const page,
    const PageNumber pageNum) {
PageFrame *framePool = (PageFrame *)bm->mgmtData;
bool bufferFilled = true;

// If the first frame in the buffer is uninitialized, set it up
while (framePool[0].pageNum == -1) {
SM_FileHandle fileHandler;
openPageFile(bm->pageFile, &fileHandler);
framePool[0].data = (SM_PageHandle) malloc(PAGE_SIZE);
ensureCapacity(pageNum, &fileHandler);
readBlock(pageNum, &fileHandler, framePool[0].data);
framePool[0].pageNum = pageNum;
framePool[0].fixCount++;
rearIndex = hit = 0;
framePool[0].hitNum = hit;
framePool[0].refNum = 0;
page->pageNum = pageNum;
page->data = framePool[0].data;

// Exit once the first frame is successfully initialized
return RC_OK;
}

// Search for an available frame or an already loaded page
int index = 0;
while (index < bufferSize && bufferFilled) {
if (framePool[index].pageNum != -1) {
if (framePool[index].pageNum == pageNum) {
  framePool[index].fixCount++;
  bufferFilled = false;
  hit++;

  switch (bm->strategy) {
      case RS_LRU:
          framePool[index].hitNum = hit;
          break;
      case RS_CLOCK:
          framePool[index].hitNum = 1;
          break;
      case RS_LFU:
          framePool[index].refNum++;
          break;
  }

  page->pageNum = pageNum;
  page->data = framePool[index].data;
  clockPointer++;
  break;
}
} else {
SM_FileHandle fileHandler;
openPageFile(bm->pageFile, &fileHandler);
framePool[index].data = (SM_PageHandle) malloc(PAGE_SIZE);
readBlock(pageNum, &fileHandler, framePool[index].data);
framePool[index].pageNum = pageNum;
framePool[index].fixCount = 1;
framePool[index].refNum = 0;
rearIndex++;
hit++;

framePool[index].hitNum = (bm->strategy == RS_LRU) ? hit : 
                        (bm->strategy == RS_CLOCK) ? 1 : 0;

page->pageNum = pageNum;
page->data = framePool[index].data;
bufferFilled = false;
break;
}
index++;
}

// If buffer is full, invoke a replacement strategy
if (bufferFilled) {
PageFrame *newFrame = (PageFrame *) malloc(sizeof(PageFrame));
SM_FileHandle fileHandler;
openPageFile(bm->pageFile, &fileHandler);
newFrame->data = (SM_PageHandle) malloc(PAGE_SIZE);
readBlock(pageNum, &fileHandler, newFrame->data);
newFrame->pageNum = pageNum;
newFrame->dirtyBit = 0;
newFrame->fixCount = 1;
newFrame->refNum = 0;
rearIndex++;
hit++;

newFrame->hitNum = (bm->strategy == RS_LRU) ? hit : 
             (bm->strategy == RS_CLOCK) ? 1 : 0;

page->pageNum = pageNum;
page->data = newFrame->data;

// Invoke the appropriate replacement strategy
switch (bm->strategy) {
case RS_FIFO:
  FIFO(bm, newFrame);
  break;
case RS_LRU:
  LRU(bm, newFrame);
  break;
case RS_CLOCK:
  CLOCK(bm, newFrame);
  break;
case RS_LFU:
  LFU(bm, newFrame);
  break;
case RS_LRU_K:
  printf("\nLRU-k algorithm is not implemented");
  break;
default:
  printf("\nUnknown algorithm specified");
}

// Successfully executed replacement strategy
return RC_OK;
}

// Successfully pinned page in an available frame
return RC_OK;
}
extern PageNumber *getFrameContents(BM_BufferPool *const bm) {
    // Allocate memory for storing page numbers dynamically
    PageNumber *frameData = (PageNumber *)malloc(sizeof(PageNumber) * bufferSize);
    PageFrame *framePtr = (PageFrame *)bm->mgmtData;

    // Initialize traversal index indirectly
    PageNumber *dataRef = frameData;
    PageFrame *currentFrame = framePtr;
    
    // Iterate over each frame and store the corresponding page number
    for (int count = 0; count < bufferSize; count++) {
        *dataRef = (currentFrame->pageNum >= 0) ? currentFrame->pageNum : NO_PAGE;
        
        // Move to the next frame and data storage slot using pointer arithmetic
        dataRef++;
        currentFrame++;
    }

    return frameData; // Return pointer to stored page numbers
}

extern bool *getDirtyFlags(BM_BufferPool *const bm) {
    // Dynamically allocate memory to store dirty bit statuses
    bool *dirtyBits = (bool *)malloc(sizeof(bool) * bufferSize);
    PageFrame *framePtr = (PageFrame *)bm->mgmtData;

    // Utilize pointer arithmetic instead of array indexing
    bool *bitRef = dirtyBits;
    PageFrame *currentFrame = framePtr;

    // Iterate through frames using an indirect approach
    for (int step = 0; step < bufferSize; step++) {
        *bitRef = (currentFrame->dirtyBit != 0); // Store the dirty status
        bitRef++;  // Move to the next dirty bit storage location
        currentFrame++;  // Move to the next frame in the buffer
    }

    return dirtyBits;  // Return pointer to the dirty flags array
}

extern int *getFixCounts(BM_BufferPool *const bufferManager) {
    // Allocate memory for storing fix count values
    int *fixCountArray = (int *)malloc(sizeof(int) * bufferSize);
    PageFrame *bufferFrames = (PageFrame *)bufferManager->mgmtData;
    
    // Using a while loop instead of a for-loop to iterate over frames
    int index = 0;
    while (index < bufferSize) {
        *(fixCountArray + index) = (bufferFrames + index)->fixCount;
        index++; // Increment counter manually
    }

    return fixCountArray; // Return the populated array
}


// Retrieves the total number of write operations performed on the buffer pool
extern int getNumWriteIO(BM_BufferPool *const bm) {
    return writeCount; // Directly returns the global write operation counter
}

// Retrieves the total number of read operations performed on the buffer pool
extern int getNumReadIO(BM_BufferPool *const bm) {
    return rearIndex + 1; // Returns the total read count based on rearIndex tracking
}

