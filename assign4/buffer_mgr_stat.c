#include "buffer_mgr_stat.h"
#include "buffer_mgr.h"
#include <stdio.h>
#include <stdlib.h>

// Internal helper function to display the replacement strategy
static void showReplacementStrategy(BM_BufferPool *const bufferPool);

/*
    // Helper functions for Main buffer manager stat functions
*/

// Define strategy constants for demonstration
#define RS_FIFO 0
#define RS_LRU 1
#define RS_CLOCK 2
#define RS_LFU 3
#define RS_LRU_K 4

void printPageStatus(PageNumber *framePages, bool *dirtyStatus, int *pinCounts, int numPages)
{
    int i = 0;

    // Using do-while loop for printing each page's status
    if (numPages > 0)
    {
        do
        {
            const char *separator = (i == 0) ? "" : ", ";
            const char *dirtyChar = (dirtyStatus[i]) ? "x" : " ";
            int currentPageFrame = framePages[i];
            int currentPinCount = pinCounts[i];

            printf("%s[%i%s%i]", separator, currentPageFrame, dirtyChar, currentPinCount);
            i++;
        } while (i < numPages);
    }
    else
    {
        printf("No pages in the buffer pool.");
    }
    printf(" \n");
}

void constructPageStatus(char *bufferDetails, size_t bufferSize, PageNumber *framePages, bool *dirtyStatus, int *pinCounts, int numPages)
{
    size_t offset = 0;
    int i = 0;

    // Using a do-while loop for constructing the page status
    do
    {
        const char *separator = (i == 0) ? "" : ",";
        const int pageFrame = framePages[i];
        const char *dirtyChar = (dirtyStatus[i]) ? "x" : " ";
        const int pinCount = pinCounts[i];

        offset += snprintf(bufferDetails + offset,
                           bufferSize - offset,
                           "%s[%i%s%i]",
                           separator,
                           pageFrame,
                           dirtyChar,
                           pinCount);
        i++;
    } while (i < numPages);
}

void printFormattedPageData(BM_PageHandle *const pageHandle)
{
    int i = 0;

    // Using a do-while loop for printing page content
    do
    {
        unsigned char currentByte = pageHandle->data[i];
        char *spaceAfter = (i + 1) % 8 == 0 ? " " : "";
        char *newLineAfter = (i + 1) % 64 == 0 ? "\n" : "";

        printf("%02X%s%s", currentByte, spaceAfter, newLineAfter);
        i++;
    } while (i < PAGE_SIZE);
}

size_t formatPageData(char *buffer, size_t remainingSize, BM_PageHandle *const pageHandle)
{
    int i = 0;
    size_t offset = 0;

    // Using a do-while loop to format each byte of page data
    do
    {
        // Format the current byte
        unsigned char currentByte = pageHandle->data[i];
        const char *spaceAfter = (i + 1) % 8 == 0 ? " " : "";
        const char *newLineAfter = (i + 1) % 64 == 0 ? "\n" : "";

        int bytesWritten = snprintf(buffer + offset,
                                    remainingSize - offset,
                                    "%02X%s%s",
                                    currentByte,
                                    spaceAfter,
                                    newLineAfter);
        if (bytesWritten < 0)
        {
            return offset; // Handle snprintf failure
        }
        offset += bytesWritten;
        i++;
    } while (i < PAGE_SIZE);
    if (true)
    {
        return offset;
    }
}

/*
    // Main buffer manager stat functions
*/

void printPoolContent(BM_BufferPool *const bufferPool)
{
    printf("{");
    PageNumber *framePages = getFrameContents(bufferPool);
    bool *dirtyStatus = getDirtyFlags(bufferPool);
    int *pinCounts = getFixCounts(bufferPool);
    showReplacementStrategy(bufferPool);


    printf(" with %i Pages}: ", bufferPool->numPages);
    // Print each page's status
    printPageStatus(framePages, dirtyStatus, pinCounts, bufferPool->numPages);
}

char *sprintPoolContent(BM_BufferPool *const bufferPool)
{
    size_t bufferSize = 250 + (20 * bufferPool->numPages);
    char *bufferDetails = (char *)malloc(bufferSize);
    if (!bufferDetails)
    {
        return NULL; // Handle memory allocation failure
    }
    else
    {
        // Fetching buffer pool statistics
        PageNumber *framePages = getFrameContents(bufferPool);
        bool *dirtyStatus = getDirtyFlags(bufferPool);
        int *pinCounts = getFixCounts(bufferPool);

        // Construct the content string
        if (bufferPool->numPages > 0)
        {
            constructPageStatus(bufferDetails, bufferSize, framePages, dirtyStatus, pinCounts, bufferPool->numPages);
        }
        else
        {
            snprintf(bufferDetails, bufferSize, "No pages in the buffer pool.");
        }

        return bufferDetails; // Caller must free the memory
    }
}

void printPageContent(BM_PageHandle *const pageHandle)
{
    printf("[Page Number: %i]\n", pageHandle->pageNum);

    // Check if page data is available
    if (PAGE_SIZE > 0)
    {
        printFormattedPageData(pageHandle);
    }
    else
    {
        printf("No data available for this page.\n");
    }
}

char *sprintPageContent(BM_PageHandle *const pageHandle)
{
    // Calculate the buffer size needed for formatted output
    size_t bufferSize = 40 + (2 * PAGE_SIZE) + (PAGE_SIZE / 64) + (PAGE_SIZE / 8);
    char *pageDetails = (char *)malloc(bufferSize);

    if (!pageDetails)
    {
        return NULL; // Handle memory allocation failure
    }

    size_t offset = 0;
    offset += snprintf(pageDetails + offset, bufferSize - offset, "[Page Number: %i]\n", pageHandle->pageNum);

    // Check if the page data is available
    if (PAGE_SIZE > 0)
    {
        offset += formatPageData(pageDetails + offset, bufferSize - offset, pageHandle);
    }
    else
    {
        snprintf(pageDetails + offset, bufferSize - offset, "No data available for this page.\n");
    }

    return pageDetails; // Caller must free the memory
}

void showReplacementStrategy(BM_BufferPool *const bufferPool)
{
    // Array of strategy names
    const char *strategyNames[] = {
        "FIFO Strategy",
        "LRU Strategy",
        "CLOCK Strategy",
        "LFU Strategy",
        "LRU-K Strategy",
        "Unknown Strategy"};

    // Get the strategy index
    int strategyIndex = bufferPool->strategy;

    // Print the strategy name based on the index
    if (strategyIndex >= 0 && strategyIndex < sizeof(strategyNames) / sizeof(strategyNames[0]))
    {
        printf("%s\n", strategyNames[strategyIndex]);
    }
    else
    {
        printf("%s\n", strategyNames[sizeof(strategyNames) / sizeof(strategyNames[0]) - 1]); // Print "Unknown Strategy"
    }
}