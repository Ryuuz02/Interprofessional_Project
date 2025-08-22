Storage Manager - File System Implementation

Team Members
• Akshith Goud Kasipuram A20561274 akasipuram@hawk.iit.edu
• Rahul Tatikonda        A20546868 rtatikonda@hawk.iit.edu
• Jacob Bode             A20489414 jbode2@hawk.iit.edu

---

Contribution Breakdown

Our team worked on every part of the project together. Everyone did an equal share of the work.

•  Akshith Goud Kasipuram - 33.33%
•  Rahul Tatikonda        - 33.33%
•  Jacob Bode             - 33.33%

---

RUNNING THE SCRIPT

1. Make sure you are in the correct directory.  
2. make clean
3. make
4. make run_test1
5. make run_test2

---

SOLUTION DESCRIPTION
==========================

The assignment aims to develop a reliable storage manager module that enables efficient data block management between memory and disk files. The module provides file-related, read-related, and write-related functionalities.

---

File-Related Functions:

➞ `initStorageManager()`
- Initializes the file stream object, setting it to `NULL`.

➞ `createPageFile(char *fileName)`
- Creates a new file and writes an empty page.  
- Returns `RC_FILE_NOT_FOUND` if unsuccessful, otherwise `RC_OK`.

➞ `openPageFile(char *fileName, SM_FileHandle *fHandle)`
- Opens an existing file in read/write mode.  
- Initializes file details and returns `RC_FILE_NOT_FOUND` on failure or `RC_OK` on success.

➞ `closePageFile(SM_FileHandle *fHandle)`
- Closes the file and sets the file stream pointer to NULL.  
- Returns `RC_OK` if successful, otherwise `RC_FILE_HANDLE_NOT_INIT`.

➞ `destroyPageFile(char *fileName)`
- Deletes the specified file using `remove()`.  
- Returns `RC_FILE_DELETE_FAILED` on failure, otherwise `RC_OK`.

---

Read-Related Functions:

➞ `readBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)`
- Validates `pageNum`, moves to the correct location using `fseek()`, and reads data into memory.

➞ `getBlockPos(SM_FileHandle *fHandle)`
- Returns the current page position (`curPagePos`) from `SM_FileHandle`.

➞ `readFirstBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)`
- Calls `readBlock(...)` with pageNum = 0 to read the first page.

➞ `readPreviousBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)`
- Calls `readBlock(...)` with pageNum = current - 1.

➞ `readCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)`
- Calls `readBlock(...)` with current page position.

➞ `readNextBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)`
- Calls `readBlock(...)` with pageNum = current + 1.

➞ `readLastBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)`
- Calls `readBlock(...)` with last page (`totalNumPages - 1`).

---

Write-Related Functions:

➞ `writeBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)`
- Checks if the page number is valid and writes data using `fwrite()`.  
- Fixed: Prevents writing beyond `totalNumPages` unless `ensureCapacity()` is explicitly called.

➞ `writeCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)`
- Calls `writeBlock(...)` with current page position.

➞ `appendEmptyBlock(SM_FileHandle *fHandle)`
- Moves to the end of the file, creates an empty block, and updates `totalNumPages`.  
- Writes the blank block to disk.

➞ `ensureCapacity(int numberOfPages, SM_FileHandle *fHandle)`
- Dynamically expands file size by appending empty blocks only when needed.

---

Key Fixes and Enhancements:
1. Fixed `writeBlock()` Behavior
   - Prevents writing beyond `totalNumPages` unless `ensureCapacity()` is explicitly called.  
   - Ensured the function returns `RC_WRITE_FAILED` if attempting to write beyond allocated space.

2. Implemented Robust Error Handling
   - `RC_READ_NON_EXISTING_PAGE` is returned when attempting to read an invalid page.  
   - `RC_FILE_NOT_FOUND` is returned when trying to open a non-existent file.  
   - `RC_FILE_DELETE_FAILED` properly handles file deletion errors.

3. Edge Case Test Coverage
   - Reading a negative page → Returns `RC_READ_NON_EXISTING_PAGE`.  
   - Writing beyond allocated pages → Returns `RC_WRITE_FAILED`.  
   - Opening a missing file → Returns `RC_FILE_NOT_FOUND`.

4. Refactored Test Cases
   - Reduced redundant test output to improve readability.  
   - Optimized assertions for better debugging insights.

5. Optimized File Expansion (`ensureCapacity()`)
   - The function now dynamically adds empty blocks when needed, preventing unnecessary file growth.

6. Created `Makefile`
   - Added separate rules for running `test1` and `test2`.  
   - Introduced a `clean` target for removing compiled files.


