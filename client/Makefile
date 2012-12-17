CC		= g++
FLAGS	= -c -D_FILE_OFFSET_BITS=64 -std=c++11 -ldb_cxx -O2
OBJS	= $(patsubst %.cpp, %.o, $(wildcard *.cpp))

all : $(OBJS)

%.o : %.cpp *.h
	$(CC) $(FLAGS) -o $@ $<

.PHONY : clean

clean :
	rm -f *.o



