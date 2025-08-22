#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "storage_mgr.h"
#include "dberror.h"
#include "test_helper.h"

// test name
char *testName;

/* test output files */
#define TESTPF "test_pagefile.bin"

/* prototypes for test functions */
static void testCreateOpenClose(void);
static void testSinglePageContent(void);

/* main function running all tests */
int main(void) {
    testName = "";

    initStorageManager();

    testCreateOpenClose();
    testSinglePageContent();

    return 0;
}

/* Test creating, opening, and closing a page file */
void testCreateOpenClose(void) {
    SM_FileHandle fh;

    testName = "test create open and close methods";

    TEST_CHECK(createPageFile(TESTPF));

    TEST_CHECK(openPageFile(TESTPF, &fh));
    ASSERT_TRUE(strcmp(fh.fileName, TESTPF) == 0, "Filename is correct");
    ASSERT_TRUE((fh.totalNumPages == 1), "Expect 1 page in a new file");
    ASSERT_TRUE((fh.curPagePos == 0), "Freshly opened file's page position should be 0");

    TEST_CHECK(closePageFile(&fh));
    TEST_CHECK(destroyPageFile(TESTPF));

    // After destruction, trying to open the file should cause an error
    ASSERT_TRUE((openPageFile(TESTPF, &fh) != RC_OK), "Opening non-existing file should return an error.");

    TEST_DONE();
}

/* Test reading and writing a single page */
void testSinglePageContent(void) {
    SM_FileHandle fh;
    SM_PageHandle ph;
    int i, zeroCount = 0;

    testName = "test single page content";

    ph = (SM_PageHandle) malloc(PAGE_SIZE);

    // Create a new page file
    TEST_CHECK(createPageFile(TESTPF));
    TEST_CHECK(openPageFile(TESTPF, &fh));
    printf("Created and opened file\n");

    // Read the first page
    TEST_CHECK(readFirstBlock(&fh, ph));

    // ✅ Check if all bytes in the first page are zero
    for (i = 0; i < PAGE_SIZE; i++) {
        if (ph[i] == 0) {
            zeroCount++;
        }
    }
    ASSERT_TRUE((zeroCount == PAGE_SIZE), "Expected all bytes in the first page to be zero");
    printf("First block was empty\n");

    // ✅ Write a known pattern to the page
    for (i = 0; i < PAGE_SIZE; i++) {
        ph[i] = (i % 10) + '0';
    }
    TEST_CHECK(writeBlock(0, &fh, ph));
    printf("Writing first block\n");

    // ✅ Read back the page and validate content
    TEST_CHECK(readFirstBlock(&fh, ph));

    int mismatchCount = 0;
    for (i = 0; i < PAGE_SIZE; i++) {
        if (ph[i] != (i % 10) + '0') {
            mismatchCount++;
        }
    }
    ASSERT_TRUE((mismatchCount == 0), "Page content does not match expected pattern.");
    printf("Reading first block\n");

    // Cleanup
    TEST_CHECK(closePageFile(&fh));  // ✅ Close file before deleting
TEST_CHECK(destroyPageFile(TESTPF));

    free(ph);

    TEST_DONE();
}
