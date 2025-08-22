# Compiler and flags
CC = gcc
CFLAGS = -I.

# Header dependencies
DEPS = buffer_mgr.h buffer_mgr_stat.h dberror.h dt.h expr.h record_mgr.h storage_mgr.h tables.h btree_mgr.h

# Object files
OBJ = storage_mgr.o dberror.o buffer_mgr_stat.o buffer_mgr.o expr.o record_mgr.o rm_serializer.o btree_mgr.o

# Generic rule for compiling object files from source files
%.o: %.c $(DEPS)
	$(CC) -c -o $@ $< $(CFLAGS)

# Default target
all: test_expr test_assign4

# Rule to compile the test_expr object file
test_expr.o: test_expr.c
	$(CC) -c test_expr.c

# Rule to compile the test_assign4_1 object file
test_assign4_1.o: test_assign4_1.c
	$(CC) -c test_assign4_1.c

# Link object files to create test_expr executable
test_expr: $(OBJ) test_expr.o
	$(CC) -o $@ $^ $(CFLAGS)

# Link object files to create test_assign4 executable
test_assign4: $(OBJ) test_assign4_1.o
	$(CC) -o $@ $^ $(CFLAGS)

# Clean up all object files and executables (Windows-compatible)
clean:
	@taskkill /F /IM test_expr.exe 2>nul || echo test_expr.exe not running
	@taskkill /F /IM test_assign4.exe 2>nul || echo test_assign4.exe not running
	@del /Q test_expr.exe test_assign4.exe *.o *~ 2>nul || echo Cleanup complete
