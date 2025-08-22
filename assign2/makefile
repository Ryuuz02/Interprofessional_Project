CC = gcc
CFLAGS = -Wall -Wextra -g

# Object files
OBJS = buffer_mgr.o storage_mgr.o dberror.o buffer_mgr_stat.o

all: build

# Compile source files into object files
buffer_mgr.o: buffer_mgr.c buffer_mgr.h
	$(CC) $(CFLAGS) -c buffer_mgr.c

storage_mgr.o: storage_mgr.c storage_mgr.h
	$(CC) $(CFLAGS) -c storage_mgr.c

dberror.o: dberror.c dberror.h
	$(CC) $(CFLAGS) -c dberror.c

buffer_mgr_stat.o: buffer_mgr_stat.c buffer_mgr_stat.h
	$(CC) $(CFLAGS) -c buffer_mgr_stat.c

test_assign2_1.o: test_assign2_1.c
	$(CC) $(CFLAGS) -c test_assign2_1.c

test_assign2_2.o: test_assign2_2.c
	$(CC) $(CFLAGS) -c test_assign2_2.c

# Build main object files without running tests
build: $(OBJS)

# Explicit test execution targets
test1: test_assign2_1
	./test_assign2_1

test2: test_assign2_2
	./test_assign2_2

test_assign2_1: $(OBJS) test_assign2_1.o
	$(CC) $(CFLAGS) -o test_assign2_1 $(OBJS) test_assign2_1.o

test_assign2_2: $(OBJS) test_assign2_2.o
	$(CC) $(CFLAGS) -o test_assign2_2 $(OBJS) test_assign2_2.o

clean:
	del /F /Q *.o test_assign2_1.exe test_assign2_2.exe 2> NUL
