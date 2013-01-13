#pragma once

#include "includes.h"

#include "buffer.h"

template <
	uint32_t BlockShift,
	typename TKey,
	typename TValue
>
class BPlusTree
{
public:
	static const uint32_t BlockSize = 1 << BlockShift;
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

	//	std::cerr << "LowerBound count " << count << std::endl;
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
//	std::vector<Node> nodes;
	typedef Buffer<BlockShift> TBuffer;
	typedef typename Buffer<BlockShift>::template BlockHolder<Node> BufferNode;
	typedef typename Buffer<BlockShift>::template ConstBlockHolder<Node> BufferNodeConst;
	TBuffer buffer;

	uint32_t limit[2];

	inline bool IsFull(TIdx idx)
	{
		BufferNodeConst node(buffer, idx);
		return node->header.count == limit[node->header.isleaf];
	}

	void SplitChild(TIdx father_id, TIdx child_id, uint32_t ith)
	{
		BufferNode father(buffer, father_id);
		BufferNode child(buffer, child_id);
		char bo = child->header.isleaf;
		TIdx newchild_id = buffer.NewBlock();
		BufferNode newchild(buffer, newchild_id);
		*newchild = Node(bo);
		assert(newchild->header.isleaf == child->header.isleaf);
		
		newchild->header.nextidx = child->header.nextidx;
		child->header.nextidx = newchild_id;

		uint32_t count = child->header.count;
		uint32_t upkeyid = count / 2;
		child->header.count = upkeyid;

		size_t off;
		TKey upkey;
		if (child->header.isleaf)
		{
			off = upkeyid * (sizeof(TKey) + sizeof(TValue));
			newchild->header.count = count - upkeyid;
			LeafNode *c = (LeafNode *)&(*child);
			upkey = c->Key(upkeyid);
			memcpy(newchild->data, child->data + off, BlockSize - off);
			assert(newchild->header.isleaf);
		}
		else
		{
			off = upkeyid * (sizeof(TKey) + sizeof(TIdx));
			newchild->header.count = count - upkeyid;
			InternalNode *c = (InternalNode *)&(*child);
			upkey = c->Key(upkeyid);
			memcpy(newchild->data, child->data + off, BlockSize - off);
			InternalNode *c2 = (InternalNode *)&(*newchild);
			c2->Key(0) = -1;
			assert(!newchild->header.isleaf);
		}

		InternalNode *f = (InternalNode *)&(*father);
		assert(f->header.count > 0);
		for (int32_t j = f->header.count - 1; j >= ith + 1; j--)
		{
			f->Key(j + 1) = f->Key(j);
			f->Val(j + 1) = f->Val(j);
		}
		f->header.count++;
		f->Key(ith + 1) = upkey;
		f->Val(ith + 1) = newchild_id;
	}

	void InsertNotFull(TIdx idx, TKey key, TValue value)
	{
	//	std::cerr << "insert not full\n";
		BufferNodeConst node(buffer, idx);
	//	fprintf(stderr, "%d  leaf %d\n", idx, node->header.isleaf);
		if (node->header.isleaf)
		{
			BufferNode node(buffer, idx);
			LeafNode *cur = (LeafNode *)&(*node);
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
			InternalNode *cur = (InternalNode *)&(*node);
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
				cur = (InternalNode *)&(*node);
				//printblk();
				if (key >= cur->Key(i + 1))
					i++;
			}
			InsertNotFull(cur->Val(i), key, value);
		}
	}

	bool _GetValue(TIdx idx, TKey key, TValue &value)
	{
		BufferNodeConst node(buffer, idx);
		if (node->header.isleaf)
		{
			auto r = (LeafNode *)&(*node);
			int32_t i = r->LowerBound(key);
			if (i < r->header.count && key == r->Key(i))
			{
				value = r->Val(i);
				return true;
			}else
				return false;
		}else
		{
			auto r = (InternalNode *)&(*node);
			int32_t i = r->LowerBound(key);
			if (i == r->header.count || key != r->Key(i))
				i--;
			return _GetValue(r->Val(i), key, value);
		}
	}



public:
	std::string fname;
	BPlusTree(std::string fname, typename TBuffer::CachePtr pcache) : fname(fname), buffer(fname, pcache)
	{
		std::ifstream froot(fname+".root");
		if (froot)
			froot >> root;
		else
		{
			TIdx node_id = buffer.NewBlock();
			BufferNode node(buffer, node_id);
			*node = Node(true);

			//fprintf(stderr, "%d  leaf %d\n", node_id, node->header.isleaf);
			assert(node->header.isleaf);
			root = 0;
		}
		uint32_t datasize = BlockSize - sizeof(typename Node::Header);
		limit[0] = datasize / (sizeof(TKey) + sizeof(TIdx));
		limit[1] = datasize / (sizeof(TKey) + sizeof(TValue));

		//std::cout << "internal limit " << limit[0] << "  leaf limit " << limit[1] << std::endl;
	}

	~BPlusTree()
	{
		std::ofstream froot(fname+".root");
		froot << root << std::endl;
	}
	void Insert(TKey key, TValue value)
	{
	//fprintf(stderr, "insert key %d value %d\n", key, value);
		if (IsFull(root))
		{
			TIdx node_id = buffer.NewBlock();
			BufferNode node(buffer, node_id);
			*node = Node(false);
			InternalNode *r = (InternalNode *)&(*node);
			r->Key(0) = -1;
			r->Val(0) = root;
			r->header.count = 1;
			SplitChild(node_id, root, 0);
			root = node_id;
			//printblk();
		}
		InsertNotFull(root , key, value);
		//printblk();
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
			std::cout << "\n" << fname << "\n";
		}
		BufferNode node(buffer, i);
		std::cout << &*node << "\t";
		uint32_t count = node->header.count;
		printf("idx %d count %d nxt %d :", i, count, node->header.nextidx);
		if (node->header.isleaf)
		{
			LeafNode *r = (LeafNode *)&(*node);
			for (uint32_t j = 0; j < count; j++)
				printf(" %d(%d)", r->Key(j), r->Val(j));
			printf("\n");
		}else
		{
			InternalNode *r = (InternalNode *)&(*node);
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
		std::shared_ptr<BufferNodeConst> pnode;
	public:
		Iterator(BPlusTree *tree, TIdx blkid, uint32_t i)
			: tree(tree), blkid(blkid), i(i), pnode(new BufferNodeConst(tree->buffer, blkid))
		{
		}
		inline bool isEnd()
		{
			return blkid == -1;
		}
		inline void Next()
		{
			auto r = (LeafNode *)&**pnode;
			i++;
			if (i >= r->header.count)
			{
				i = 0;
				blkid = r->header.nextidx;
				if (blkid != -1)
					pnode.reset(new BufferNodeConst(tree->buffer, blkid));
			}
		}
		inline const TKey Key()
		{
			auto r = (LeafNode *)&**pnode;
			return r->Key(i);
		}
		inline const TValue Val()
		{
			auto r = (LeafNode *)&**pnode;
			return r->Val(i);
		}

	};

	Iterator LowerBound(TKey key)
	{
//		std::cerr << "LowerBound \n";
//		printblk();
		TIdx idx = root;
		std::unique_ptr<BufferNodeConst> pnode(new BufferNodeConst(buffer, idx));
		while (!(*pnode)->header.isleaf)
		{
			auto r = (InternalNode *)&(**pnode);
			int32_t i = r->LowerBound(key);
			i--;
			idx = r->Val(i);
			pnode.reset(new BufferNodeConst(buffer, idx));
		}

		auto r = (LeafNode *)&(**pnode);
		int32_t i = r->LowerBound(key);
		Iterator ret(this, idx, i);
		if (i >= r->header.count)
			ret.Next();
		return ret;
	}
};
