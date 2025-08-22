Buffer Manager Implementation

Team Members
• Akshith Goud Kasipuram A20561274 akasipuram@hawk.iit.edu
• Rahul Tatikonda        A20546868 rtatikonda@hawk.iit.edu
• Jacob Bode             A20489414 jbode2@hawk.iit.edu
• Prithwee reddei patelu A20560828 ppatelu@hawk.iit.edu
---

Contribution Breakdown:

Our team worked on every part of the project together. Everyone did an equal share of the work.

•  Akshith Goud Kasipuram - 25%
•  Rahul Tatikonda        - 25%
•  Jacob Bode             - 25%
• Prithwee reddei patelu  - 25%

RUNNING THE SCRIPT:

1. Make sure you are in the correct directory.  
2. make clean
3. make
4. make test1
5. make test2

Output Video Link(Loom): https://www.loom.com/share/d916a5f0e6164a35b28b6191a20b4198?sid=022169ce-5cfa-4fd8-a3fc-7413c0692946
Overview:

This project implements a buffer manager to handle page replacement and memory management in a database system. It provides functionality for different page replacement strategies, tracking memory usage, and efficiently managing disk operations.

Functions Implemented:

1. Page Replacement Strategies
- FIFO (First-In-First-Out)
  - Replaces the oldest page in the buffer pool.
  - If a dirty page is found, it is written to disk before being replaced.
  - Uses rearIndex to track the oldest page.
  - Ensures fair allocation by replacing pages in the order they were loaded.

- LFU (Least Frequently Used)
  - Selects the page with the lowest reference count.
  - Ensures that a pinned page is not replaced.
  - If the least frequently used page is dirty, it is written back to disk.
  - Updates lfuPtr to track the least frequently used page.

- LRU (Least Recently Used)
  - Selects the least recently accessed page.
  - Uses hitNum to track how recently a page was accessed.
  - Writes dirty pages back to disk before replacement.
  - Prevents frequently accessed pages from being replaced too soon.

- CLOCK (Clock Algorithm)
  - Uses clockPointer to track pages in a circular fashion.
  - If a page is unpinned, it replaces it immediately.
  - If all pages are pinned, it resets their usage and makes another pass.

2. Buffer Pool Management
- initBufferPool
  - Initializes the buffer pool with the given number of frames.
  - Allocates memory for page frames and assigns a page replacement strategy.
  - Resets all global tracking variables.

- shutdownBufferPool
  - Ensures all dirty pages are written to disk before shutting down.
  - Checks for any pinned pages and prevents shutdown if any exist.
  - Frees allocated memory for the buffer pool.

- forceFlushPool
  - Writes all dirty pages (unpinned) back to disk.
  - Iterates through the buffer and flushes only modified pages.

3. Page Management Functions
- markDirty
  - Marks a specific page in the buffer pool as modified (dirty).

- unpinPage
  - Decreases the fix count of a page, indicating it is no longer in use.

- forcePage
  - Writes a specific page back to disk, even if it is pinned.

- pinPage
  - Loads a page into the buffer pool.
  - If the page is already in memory, its fix count is increased.
  - If the buffer is full, it applies the chosen page replacement strategy.

4. Statistics and Utility Functions
- getFrameContents
  - Returns an array of page numbers currently stored in the buffer.

- getDirtyFlags
  - Returns an array indicating which pages are dirty.

- getFixCounts
  - Returns an array containing the fix count for each page.

- getNumWriteIO
  - Returns the total number of write operations performed.

- getNumReadIO
  - Returns the total number of read operations performed.

How It Works
1. The buffer manager starts by initializing a buffer pool using initBufferPool.
2. When a page request is made via pinPage, the system checks if the page is already in memory.
3. If the page is not found, it loads the page from disk and applies a page replacement strategy if necessary.
4. unpinPage and markDirty are used to modify and release pages when needed.
5. forceFlushPool ensures dirty pages are saved before shutdown.
6. The system tracks the number of read and write operations using getNumReadIO and getNumWriteIO.

Conclusion:
This buffer manager efficiently manages page replacements and memory in a database system. It provides different page replacement algorithms, such as FIFO and LRU, and ensures efficient disk I/O operations while keeping track of page modifications and memory usage.


