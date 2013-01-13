#pragma once

#include "includes.h"

extern char EmptyBlock[IndexBlockSize];

template <size_t BlockSize>
class Cache
{
public:
	typedef uint32_t BlkId;
	typedef std::pair<int,off_t> Tag;
	size_t size;
	void *mem;
	BlkId nblocks;
	size_t misscount, hitcount;
	struct Info
	{
		Tag tag;
		bool dirty;
		int refnum;
		Info *next, *prev;
		Info(int fd = -1, off_t off = 0) : tag(fd, off), dirty(false), refnum(0), next(nullptr), prev(nullptr) {}
//		Info(Tag tag) : tag(tag), dirty(false), refnum(0), next(nullptr), prev(nullptr) {}
	};
	std::vector<Info> info;
	Info *head, *tail;
	std::map<Tag, BlkId> mapping;

	inline void Push(BlkId id)
	{
		Info *cur = &info[id];
		if (tail == nullptr)
		{
			assert(head == nullptr);
			head = tail = cur;
			cur->next = nullptr;
			cur->prev = nullptr;
		}else
		{
			tail->next = cur;
			cur->prev = tail;
			cur->next = nullptr;
			tail = cur;
		}
	}
	inline void Pop(BlkId id)
	{
		Info *cur = &info[id];

		if (cur->next == nullptr)
			tail = cur->prev;
		else
			cur->next->prev = cur->prev;

		if (cur->prev == nullptr)
			head = cur->next;
		else
			cur->prev->next = cur->next;

		cur->prev = cur->next = nullptr;
	}
	inline void NewRef(BlkId id)
	{
		if (info[id].refnum++ == 0)
			Pop(id);
	}
	inline void DelRef(BlkId id)
	{
		if (--info[id].refnum == 0)
			Push(id);
	}
	inline void DoRead(BlkId id)
	{
		info[id].dirty = false;
		lseek(info[id].tag.first, info[id].tag.second, SEEK_SET);
		ssize_t ret = read(info[id].tag.first, (char*)mem + id * BlockSize, BlockSize);
		if (ret != BlockSize)
		{
			fprintf(stderr, "Read fd %d off %d error ret = %d errno = %d\n", info[id].tag.first, info[id].tag.second, ret, errno);
		}
		assert(ret == BlockSize);
//		fprintf(stderr, "Can't read %d\n", id);
	}
	inline void DoWrite(BlkId id)
	{
		info[id].dirty = false;
		lseek(info[id].tag.first, info[id].tag.second, SEEK_SET);
		ssize_t ret = write(info[id].tag.first, (char*)mem + id * BlockSize, BlockSize);
		assert(ret == BlockSize);
//		fprintf(stderr, "Can't write %d\n", id);
		//lseek(fd, fsize, SEEK_SET);
	}

public:
	Cache(size_t size) : size(size), misscount(0), hitcount(0)
	{
		mem = malloc(size);
//	fprintf(stderr, "CACHE CREATED AT %x\n", mem);
		assert(mem > 0);
		nblocks = size / BlockSize;
		info.resize(nblocks);
		head = tail = nullptr;
		for (BlkId i = 0; i < nblocks; i++)
			Push(i);
	}
	void Print()
	{
		fprintf(stderr, "CACHE FREE : Size = %lld hitcount = %lld misscount = %lld rate = %lf\n", size, hitcount, misscount, (double)hitcount / (misscount + hitcount));
	}
	~Cache()
	{
		Print();
		free(mem);
	}
	class BlockPtr
	{
		BlkId blkid;
		Cache* cache;
		bool &dirty;
	public:
		char* p;
		BlockPtr(Cache* cache, BlkId blkid) : cache(cache), blkid(blkid), dirty(cache->info[blkid].dirty)
		{
			p = (char *)cache->mem + blkid * BlockSize;
			cache->NewRef(blkid);
		}
		~BlockPtr()
		{
			cache->DelRef(blkid);
		}
		void SetDirty()
		{
			dirty = true;
		//	printf("set dirty %d  : %d %d\n", blkid, dirty, cache->info[blkid].dirty);
		}
	};
	BlockPtr Query(int fd, off_t off)
	{
	//printf("Query %d %d\n",fd ,off);while(1);
		Tag tag(fd, off);
		auto it = mapping.find(tag);
		BlkId blkid;
		if (it != mapping.end())
		{
	//printf("hit!");

			hitcount ++;
			blkid = it->second;
		}else
		{
	//printf("miss!");
			misscount++;
			while (head == nullptr)
			{
				std::cerr << "Cache is full" << std::endl;
			}
			blkid = head - &info[0];
			mapping.erase(head->tag);
			if (info[blkid].dirty)
				DoWrite(blkid);
			Tag newtag(fd, off);
			info[blkid].tag = newtag;
			mapping[newtag] = blkid;
			DoRead(blkid);
		}
	//printf(" blkid is %d\n", blkid);
		return BlockPtr(this, blkid);
	};
	void Flushfd(int fd)
	{
		for (BlkId i = 0; i < nblocks; i++)
		{
			if (info[i].tag.first == fd && info[i].dirty)
			{
				DoWrite(i);
			}
			//if (info[i].tag.first == fd)printf("%d : %d\n", i, info[i].dirty);
		}
	}
};


template <uint32_t BlockShift>
class Buffer
{
public:
	static const uint32_t BlockSize = 1 << BlockShift;
	static const uint32_t BlockOffMask = BlockSize - 1;
	static const uint32_t BlockIdMask = ~BlockOffMask;
	typedef Cache<BlockSize> TCache;
	typedef std::shared_ptr<TCache> CachePtr;
	typedef typename TCache::BlockPtr BlockPtr;

private:
	CachePtr pcache;
	std::string fname;
	int fd;
	size_t fsize;
	Buffer(const Buffer &);
	Buffer(const Buffer&&);
public:

	Buffer(std::string fname, CachePtr pcache) : fname(fname), pcache(pcache)
	{
		fd = open(fname.c_str(), O_RDWR | O_CREAT, 0666);
		struct stat st;
		fprintf(stderr, "open %s = %d\n", fname.c_str(), fd);
		if (stat(fname.c_str(), &st) == 0)
		{
			fsize = lseek(fd, 0, SEEK_END);
			std::cerr << fname << " size : " << fsize << std::endl;
		}else
		{
			fsize = 0;
			std::cerr << "stat failed\n" << std::endl;
		}

	}
	~Buffer()
	{
		pcache->Flushfd(fd);
	//	fprintf(stderr, "close = %d\n", fd);
		close(fd);
	}
	off_t Append(const void *buf, const size_t len)
	{
		assert(len <= BlockSize);
		if (((fsize - 1) & BlockIdMask) != ((fsize + len - 1) & BlockIdMask))
		{
			// New Block
			char EmptyBlock[BlockSize];
			memset(EmptyBlock, 0, BlockSize);
			lseek(fd, 0, SEEK_END);
			ssize_t ret = write(fd, EmptyBlock, BlockSize);
			if (ret != BlockSize)
			{
				fprintf(stderr, "WRITE fd %d off %d error ret = %d errno = %d\n",fd, fsize, ret, errno);
				assert(0);
			}

			fsize = (fsize + len - 1) & BlockIdMask;
		}
		off_t ret = fsize;
		auto block = pcache->Query(fd, fsize & BlockIdMask);
		memcpy(block.p + (fsize & BlockOffMask), buf, len);
		fsize += len;
		block.SetDirty();
		return ret;
	}
	size_t NewBlock()
	{
		off_t off = Append(EmptyBlock, BlockSize);
		return off / BlockSize;
	}
	template <typename T>
	T* Get(off_t off)
	{
		auto block = pcache->Query(fd, off & BlockIdMask);
		return block.p + (off & BlockOffMask);
	}
/*	void chk(const char *s)
	{
		fprintf(stderr, s);
		fprintf(stderr, " %x\n", *(int*)pcache->mem);
	}*/

	template <typename T>
	class BlockHolder
	{
		BlockPtr block;
		T * pointer;
		BlockHolder(BlockHolder &) = delete;
	public:
		BlockHolder(Buffer &buffer, size_t numblk, off_t off = 0) :
			block(buffer.pcache->Query(buffer.fd, numblk * BlockSize))
		{
			block.SetDirty();
			pointer = (T*)(block.p + off);
		}
		T* operator ->()
		{
			return pointer;
		}
		T& operator *()
		{
			return *pointer;
		}
		const T* operator ->() const
		{
			return pointer;
		}
		const T& operator *() const
		{
			return *pointer;
		}
	};
	template <typename T>
	class ConstBlockHolder
	{
		BlockPtr block;
		T * pointer;
		ConstBlockHolder(ConstBlockHolder &) = delete;
	public:
		ConstBlockHolder(Buffer &buffer, size_t numblk, off_t off = 0) :
			block(buffer.pcache->Query(buffer.fd, numblk * BlockSize))
		{
			pointer = (T*)(block.p + off);
		}

		const T* operator ->()
		{
			return pointer;
		}
		const T& operator *()
		{
			return *pointer;
		}
	};
/*
	template <typename T>
	BlockHolder<T> Hold(size_t numblk, off_t off = 0)
	{
		BlockPtr block = pcache->Query(fd, numblk * BlockSize);
		block.SetDirty();
		return BlockHolder<T>(std::move(block), off);
	}
	template <typename T>
	const BlockHolder<T> ConstHold(size_t numblk, off_t off = 0)
	{
		BlockPtr block = pcache->Query(fd, numblk * BlockSize);
		return BlockHolder<T>(std::move(block), off);
	}*/
};
extern std::shared_ptr<Cache<RawBlockSize>> pcache;
extern std::shared_ptr<Cache<IndexBlockSize>> pcacheindex;
