CC		= g++
FLAGS	= -c -D_FILE_OFFSET_BITS=64 -std=c++11 -O2
OBJS	= $(patsubst %.cpp, %.o, $(wildcard *.cpp))

all : $(OBJS)
	ar x *.a
	#$(MAKE) -j4 --directory=./db/build_unix

%.o : %.cpp *.h
	$(CC) $(FLAGS) -o $@ $<

.PHONY : clean

clean :
	#$(MAKE) --directory=./tool clean
	#$(MAKE) --directory=./db/build_unix clean
	rm -f *.o



