#pragma once

#include "includes.h"

template <
	size_t BlockSize,
	typename TKey,
	typename TValue
>
class BPlusTree
{
public:
	typedef uint32_t TIdx;
	typedef TKey Tkey_decl;
	typedef TValue TValue_decl;
	private:

	class Node
	{
	public:
		struct Header
		{
			char isleaf;
			uint32_t count;
			TIdx nextidx;
		};
		Header header;
		uint8_t data[BlockSize - sizeof(Header)];
		Node(char isleaf)
		{
			header.isleaf = isleaf;
			header.count = 0;
			header.nextidx = -1;
		}

	};
	class InternalNode : public Node
	{
	public:
		inline TKey & Key(uint32_t idx)
		{
			assert(idx * (sizeof(TKey) + sizeof(TIdx)) < sizeof(this->data));
			return *reinterpret_cast<TKey*>(this->data + idx * (sizeof(TKey) + sizeof(TIdx)));
		}
		inline TIdx & Val(uint32_t idx)
		{
			assert(idx * (sizeof(TKey) + sizeof(TIdx)) + sizeof(TKey) < sizeof(this->data));
			return *reinterpret_cast<TIdx*>(this->data + idx * (sizeof(TKey) + sizeof(TIdx)) + sizeof(TKey));
		}
		inline uint32_t LowerBound(TKey key)
		{
			uint32_t count = this->header.count - 1;
			uint32_t first = 1;
			while (count > 0)
			{
				uint32_t step = count / 2;
				TKey mid = Key(first + step);
				if (mid < key)
				{
					first += step + 1;
					count -= step + 1;
				} else
					count = step;
			}
			return first;
		}
	};
	class LeafNode : public Node
	{
	public:
		inline TKey & Key(uint32_t idx)
		{
			assert(idx * (sizeof(TKey) + sizeof(TValue)) < sizeof(this->data));
			return *reinterpret_cast<TKey*>(this->data + idx * (sizeof(TKey) + sizeof(TValue)));
		}
		inline TValue & Val(uint32_t idx)
		{
			assert(idx * (sizeof(TKey) + sizeof(TValue)) + sizeof(TKey) < sizeof(this->data));
			return *reinterpret_cast<TValue*>(this->data + idx * (sizeof(TKey) + sizeof(TValue)) + sizeof(TKey));
		}
		inline uint32_t LowerBound(TKey key)
		{
			uint32_t count = this->header.count;
			uint32_t first = 0;
			while (count > 0)
			{
				uint32_t step = count / 2;
				TKey mid = Key(first + step);
				if (mid < key)
				{
					first += step + 1;
					count -= step + 1;
				} else
					count = step;
			}
			return first;
		}
	};
	TIdx root;
	std::vector<Node> nodes;

	uint32_t limit[2];

	inline bool IsFull(TIdx idx)
	{
		return nodes[idx].header.count == limit[nodes[idx].header.isleaf];
	}

	void SplitChild(TIdx father, TIdx child, uint32_t ith)
	{
		uint32_t newchild = nodes.size();
		char bo = nodes[child].header.isleaf;
		nodes.emplace_back(bo);
		assert(nodes[newchild].header.isleaf == nodes[child].header.isleaf);
		
		nodes[newchild].header.nextidx = nodes[child].header.nextidx;
		nodes[child].header.nextidx = newchild;

		uint32_t count = nodes[child].header.count;
		uint32_t upkeyid = count / 2;
		nodes[child].header.count = upkeyid;

		size_t off;
		TKey upkey;
		if (nodes[child].header.isleaf)
		{
			off = upkeyid * (sizeof(TKey) + sizeof(TValue));
			nodes[newchild].header.count = count - upkeyid;
			LeafNode *c = (LeafNode *)&nodes[child];
			upkey = c->Key(upkeyid);
			memcpy(nodes[newchild].data, nodes[child].data + off, BlockSize - off);
			assert(nodes[newchild].header.isleaf);
		}
		else
		{
			off = upkeyid * (sizeof(TKey) + sizeof(TIdx));
			nodes[newchild].header.count = count - upkeyid;
			InternalNode *c = (InternalNode *)&nodes[child];
			upkey = c->Key(upkeyid);
			memcpy(nodes[newchild].data, nodes[child].data + off, BlockSize - off);
			InternalNode *c2 = (InternalNode *)&nodes[newchild];
			c2->Key(0) = -1;
			assert(!nodes[newchild].header.isleaf);
		}

		InternalNode *f = (InternalNode *)&nodes[father];
		assert(f->header.count > 0);
		for (int32_t j = f->header.count - 1; j >= ith + 1; j--)
		{
			f->Key(j + 1) = f->Key(j);
			f->Val(j + 1) = f->Val(j);
		}
		f->header.count++;
		f->Key(ith + 1) = upkey;
		f->Val(ith + 1) = newchild;
	}

	void InsertNotFull(TIdx idx, TKey key, TValue value)
	{
		if (nodes[idx].header.isleaf)
		{
			LeafNode *cur = (LeafNode *)&nodes[idx];
			int32_t i = cur->header.count - 1;
			while (i >= 0 && key < cur->Key(i))
			{
				cur->Key(i + 1) = cur->Key(i);
				cur->Val(i + 1) = cur->Val(i);
				i--;
			}
			i++;
			cur->Key(i) = key;
			cur->Val(i) = value;
			cur->header.count++;
		}else
		{
			InternalNode *cur = (InternalNode *)&nodes[idx];
 			/*int32_t i = cur->header.count - 1;
			while (i > 0 && key < cur->Key(i))
			{
				i--;
			}*/
			int32_t i = cur->LowerBound(key);
			i--;
			uint32_t next_idx = cur->Val(i);
			if (IsFull(next_idx))
			{
				SplitChild(idx, next_idx, i);
				cur = (InternalNode *)&nodes[idx];
				//printblk();
				if (key >= cur->Key(i + 1))
					i++;
			}
			InsertNotFull(cur->Val(i), key, value);
		}
	}

	bool _GetValue(TIdx idx, TKey key, TValue &value)
	{
		if (nodes[idx].header.isleaf)
		{
			auto r = (LeafNode *)&nodes[idx];
			int32_t i = r->LowerBound(key);
			if (i < r->header.count && key == r->Key(i))
			{
				value = r->Val(i);
				return true;
			}else
				return false;
		}else
		{
			auto r = (InternalNode *)&nodes[idx];
			int32_t i = r->LowerBound(key);
			if (i == r->header.count || key != r->Key(i))
				i--;
			return _GetValue(r->Val(i), key, value);
		}
	}



public:

	BPlusTree()
	{
		nodes.emplace_back(true);
		root = 0;
		uint32_t datasize = BlockSize - sizeof(typename Node::Header);
		limit[0] = datasize / (sizeof(TKey) + sizeof(TIdx));
		limit[1] = datasize / (sizeof(TKey) + sizeof(TValue));

		//std::cout << "internal limit " << limit[0] << "  leaf limit " << limit[1] << std::endl;
	}

	void Insert(TKey key, TValue value)
	{
		if (IsFull(root))
		{
			uint32_t newroot = nodes.size();
			nodes.emplace_back(false);
			InternalNode *r = (InternalNode *)&nodes[newroot];
			r->Key(0) = -1;
			r->Val(0) = root;
			r->header.count = 1;
			SplitChild(newroot, root, 0);
			root = newroot;
			//printblk();
		}
		InsertNotFull(root , key, value);
	}

	bool GetValue(TKey key, TValue &value)
	{
		return _GetValue(root, key, value);
	}

	void printblk(TIdx i = -1)
	{
		if (i == -1)
		{
			i = root;
			printf("\nBtree\n");
		}
		uint32_t count = nodes[i].header.count;
		printf("idx %d count %d nxt %d :", i, count, nodes[i].header.nextidx);
		if (nodes[i].header.isleaf)
		{
			LeafNode *r = (LeafNode *)&nodes[i];
			for (uint32_t j = 0; j < count; j++)
				printf(" %d(%d)", r->Key(j), r->Val(j));
			printf("\n");
		}else
		{
			InternalNode *r = (InternalNode *)&nodes[i];
			for (uint32_t j = 0; j < count; j++)
				printf(" %d[%d]", r->Key(j), r->Val(j));
			printf("\n");
			for (uint32_t j = 0; j < count; j++)
				printblk(r->Val(j));
		}
	}
public:
	class Iterator
	{
		BPlusTree *tree;
		TIdx blkid;
		uint32_t i;
	public:
		Iterator(BPlusTree *tree, TIdx blkid, uint32_t i)
			: tree(tree), blkid(blkid), i(i)
		{
		}
		inline bool isEnd()
		{
			return blkid == -1;
		}
		inline void Next()
		{
			auto r = (LeafNode *)&tree->nodes[blkid];
			i++;
			if (i >= r->header.count)
			{
				i = 0;
				blkid = r->header.nextidx;
			}
		}
		inline TKey Key()
		{
			auto r = (LeafNode *)&tree->nodes[blkid];
			return r->Key(i);
		}
		inline TValue Val()
		{
			auto r = (LeafNode *)&tree->nodes[blkid];
			return r->Val(i);
		}

	};

	Iterator LowerBound(TKey key)
	{
		TIdx idx = root;
		while (!nodes[idx].header.isleaf)
		{
			auto r = (InternalNode *)&nodes[idx];
			int32_t i = r->LowerBound(key);
			i--;
			idx = r->Val(i);
		}

		auto r = (LeafNode *)&nodes[idx];
		int32_t i = r->LowerBound(key);
		Iterator ret(this, idx, i);
		if (i >= r->header.count)
			ret.Next();
		return ret;
	}
};
