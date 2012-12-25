CC		= g++
FLAGS	= -O2 -D_FILE_OFFSET_BITS=64

.PHONY : all clean tar

all :
	$(MAKE) --directory=client
	$(CC) $(FLAGS) -o run main.cpp hash.cpp client/*.o workload.cpp -lpthread 
	#client/db/build_unix/*.o

clean :
	$(MAKE) --directory=client clean
	rm -f run *.tar.gz data/*

tar :
	tar zcvf submission.tar.gz client



