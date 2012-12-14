CC		= g++
FLAGS	= -O3 -c -D_FILE_OFFSET_BITS=64 -std=c++11
OBJS	= $(patsubst %.cpp, %.o, $(wildcard *.cpp))

all : $(OBJS)

%.o : %.cpp *.h
	$(CC) $(FLAGS) -o $@ $<

.PHONY : clean

clean :
	rm -f *.o



