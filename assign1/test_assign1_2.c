#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "storage_mgr.h"
#include "dberror.h"
#include "test_helper.h"

// Test name
char *testName;

#define TESTPF "test_pagefile_2.bin"

/* Prototypes */
static void testEnsureCapacity(void);
static void testAppendEmptyBlock(void);
static void testMultipleReadsWrites(void);
static void testReadNegativePage(void);
static void testWriteBeyondCapacity(void);
static void testOpenNonExistingFile(void);

/* Main function running all tests */
int main(void) {
    testName = "";

    initStorageManager();

    testEnsureCapacity();
    testAppendEmptyBlock();
    testMultipleReadsWrites();
    testReadNegativePage();
    testWriteBeyondCapacity();
    testOpenNonExistingFile();

    return 0;
}

/* Test `ensureCapacity()` expands file properly */
void testEnsureCapacity(void) {
    SM_FileHandle fh;

    testName = "test ensureCapacity()";

    TEST_CHECK(createPageFile(TESTPF));
    TEST_CHECK(openPageFile(TESTPF, &fh));

    // Ensure file expands to at least 10 pages
    TEST_CHECK(ensureCapacity(10, &fh));
    ASSERT_TRUE((fh.totalNumPages >= 10), "File expanded to 10 pages");

    TEST_CHECK(closePageFile(&fh));
    TEST_CHECK(destroyPageFile(TESTPF));

    TEST_DONE();
}

/* Test `appendEmptyBlock()` adds a new page */
void testAppendEmptyBlock(void) {
    SM_FileHandle fh;

    testName = "test appendEmptyBlock()";

    TEST_CHECK(createPageFile(TESTPF));
    TEST_CHECK(openPageFile(TESTPF, &fh));

    int initialPages = fh.totalNumPages;
    TEST_CHECK(appendEmptyBlock(&fh));

    ASSERT_TRUE((fh.totalNumPages == initialPages + 1), "New block appended correctly");

    TEST_CHECK(closePageFile(&fh));
    TEST_CHECK(destroyPageFile(TESTPF));

    TEST_DONE();
}

/* Test `writeBlock()` and `readBlock()` */
void testMultipleReadsWrites(void) {
    SM_FileHandle fh;
    SM_PageHandle ph;
    int i;

    testName = "test multiple reads and writes";

    ph = (SM_PageHandle) malloc(PAGE_SIZE);

    TEST_CHECK(createPageFile(TESTPF));
    TEST_CHECK(openPageFile(TESTPF, &fh));

    // Expand file to 5 pages
    TEST_CHECK(ensureCapacity(5, &fh));

    // Write data to each page
    for (int page = 0; page < 5; page++) {
        for (i = 0; i < PAGE_SIZE; i++) {
            ph[i] = (page % 10) + '0';
        }
        TEST_CHECK(writeBlock(page, &fh, ph));
    }

    // Read back and verify
    for (int page = 0; page < 5; page++) {
        TEST_CHECK(readBlock(page, &fh, ph));
        int mismatchCount = 0;
        for (i = 0; i < PAGE_SIZE; i++) {
            if (ph[i] != (page % 10) + '0') {
                mismatchCount++;
            }
        }
        ASSERT_TRUE((mismatchCount == 0), "Page content matches written data.");
    }

    free(ph);
    TEST_CHECK(closePageFile(&fh));
    TEST_CHECK(destroyPageFile(TESTPF));

    TEST_DONE();
}

/* Test reading a negative page */
void testReadNegativePage(void) {
    SM_FileHandle fh;
    SM_PageHandle ph;

    testName = "test read negative page";

    ph = (SM_PageHandle) malloc(PAGE_SIZE);

    TEST_CHECK(createPageFile(TESTPF));
    TEST_CHECK(openPageFile(TESTPF, &fh));

    ASSERT_TRUE((readBlock(-1, &fh, ph) == RC_READ_NON_EXISTING_PAGE),
                "Reading a negative page number should fail");

    TEST_CHECK(closePageFile(&fh));
    TEST_CHECK(destroyPageFile(TESTPF));
    free(ph);

    TEST_DONE();
}

/* Test writing beyond totalNumPages without `ensureCapacity()` */
void testWriteBeyondCapacity(void) {
    SM_FileHandle fh;
    SM_PageHandle ph;
    int i;

    testName = "test write beyond file size";

    ph = (SM_PageHandle) malloc(PAGE_SIZE);

    TEST_CHECK(createPageFile(TESTPF));
    TEST_CHECK(openPageFile(TESTPF, &fh));

    for (i = 0; i < PAGE_SIZE; i++) {
        ph[i] = 'X';
    }

    ASSERT_TRUE((writeBlock(100, &fh, ph) == RC_WRITE_FAILED),
                "Writing beyond totalNumPages without `ensureCapacity()` should fail");

    TEST_CHECK(closePageFile(&fh));
    TEST_CHECK(destroyPageFile(TESTPF));
    free(ph);

    TEST_DONE();
}

/* Test opening a non-existing file */
void testOpenNonExistingFile(void) {
    SM_FileHandle fh;

    testName = "test open non-existing file";

    ASSERT_TRUE((openPageFile("non_existent.bin", &fh) == RC_FILE_NOT_FOUND),
                "Opening a non-existing file should return RC_FILE_NOT_FOUND");

    TEST_DONE();
}
