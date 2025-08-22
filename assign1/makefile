# Compiler and flags
CC = gcc
CFLAGS = -Wall -g

# Object files list
OBJS = storage_mgr.o dberror.o test_assign1_1.o test_assign1_2.o

# Compile all C files into .o files
storage_mgr.o: storage_mgr.c storage_mgr.h
	$(CC) -c storage_mgr.c -o storage_mgr.o

dberror.o: dberror.c dberror.h
	$(CC) -c dberror.c -o dberror.o

test_assign1_1.o: test_assign1_1.c test_helper.h
	$(CC) -c test_assign1_1.c -o test_assign1_1.o

test_assign1_2.o: test_assign1_2.c test_helper.h
	$(CC) -c test_assign1_2.c -o test_assign1_2.o

# Linking step - Creates the executables
test1: storage_mgr.o dberror.o test_assign1_1.o
	$(CC) $(CFLAGS) -o test1 storage_mgr.o dberror.o test_assign1_1.o

test2: storage_mgr.o dberror.o test_assign1_2.o
	$(CC) $(CFLAGS) -o test2 storage_mgr.o dberror.o test_assign1_2.o

# Rules for Running Test Cases
run_test1: test1
	./test1

run_test2: test2
	./test2

# Clean compiled files (Windows Compatible)
clean:
	del /F /Q test1.exe test2.exe *.o 2>NUL || exit 0
