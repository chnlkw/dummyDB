#include "btree.h"

#include "dummydb.h"
#include "buffer.h"

hash<string> hash_str;

class RawItem
{
public:
	size_t size;
	const char *buf;
	RawItem(const char *buf = nullptr, const size_t size = 0) : size(size), buf(buf) {	}
	RawItem(DummyItem & item)
	{
		size = item.intdata.size() * sizeof(int);
		for (auto &str : item.strdata)
		{
			size += str.length() + 1;
		}
		buf = new char[size];
		int *p = (int*)buf;
		for (auto &i : item.intdata)
		{
			*p = i;
			p++;
		}
		char *s = (char *)p;
		for (auto &str : item.strdata)
		{
			strcpy(s, str.c_str());
			s += str.length();
			*s = '\0';
			s++;
		}
	}
	~RawItem()
	{
		delete [] buf;
	}
};

DummyItem to_item(void * buf, int nInt, int nStr)
{
	DummyItem item;
	int *p = (int*)buf;
	for (int i = 0; i < nInt; i++)
	{
//fprintf(stderr, "to_item get int %x\n", p);
		item.intdata.push_back(*p);
		p++;

	}
	char *s = (char*)p;
	for (int i = 0; i < nStr; i++)
	{
//fprintf(stderr, "to_item get str %s\n", s);
		item.strdata.push_back(s);
		s += strlen(s) + 1;
	}
	return item;
}


class BTreeTable : public BaseTable
{
public:
	typedef BPlusTree<RawBlockShift, int, off_t> TBTreeIntKey;
	typedef BPlusTree<RawBlockShift, size_t, off_t> TBTreeStrKey;
	typedef Buffer<RawBlockShift> TBuffer;

	TBuffer rawdata;
	int sum;

public:
	vector<unique_ptr<TBTreeIntKey>> IntKey;
	vector<unique_ptr<TBTreeStrKey>> StrKey;

	BTreeTable(string tablename, int nInt, int nIntKey, int nStr, int nStrKey, vector<int>& StringTypeLen) :
		rawdata(string("data/")+tablename+".bin", pcache),
		BaseTable(nInt, nIntKey, nStr, nStrKey, StringTypeLen)
	{
		for (int i = 0; i < nIntKey; i++) {
			IntKey.emplace_back(new TBTreeIntKey(string("data/")+tablename+".int_"+std::to_string(i), pcache));
		}
		for (int i = 0; i < nStrKey; i++) {
			StrKey.emplace_back(new TBTreeStrKey(string("data/")+tablename+".str_"+std::to_string(i), pcache));
		}
	}
	bool Insert(DummyItem &item)
	{
		RawItem tmp(item);
		off_t off = rawdata.Append(tmp.buf, tmp.size);
//fprintf(stderr, "insert at %d\n", off);
		for (int i = 0; i < nIntKey; i++) {
			IntKey[i]->Insert(item.intdata[i], off);
		}
		for (int i = 0; i < nStrKey; i++)
			StrKey[i]->Insert(hash_str(item.strdata[i]), off);
		//data.push_back(item);
		sum++;
		return true;
	}

	virtual const DummyItem GetData(int index) const override
	{
		assert(0);
	}
	virtual const int GetDataSize() const override
	{
		return sum;
	}
/*
	void chk(const char * s)
	{
		rawdata.chk(s);
	}
*/
//	virtual const multimap<int, int>& GetIntKey(int index) override;
//	virtual const unordered_multimap<string, int>& GetStrKey(int index) override;
	template <typename TT>
	class BTreeCursor : public Cursor
	{
		typedef typename TT::Tkey_decl TKey;
		typedef typename TT::TValue_decl TValue;
		typedef typename TT::Iterator TIter;

		TIter iter;
		TKey maxkey;
		TBuffer &rawdata;
		int nInt, nStr;
	public:
		BTreeCursor(TBuffer &rawdata, int nInt, int nStr, TIter &&iter, TKey maxkey, DummyQuery q = DummyQuery())
			: rawdata(rawdata), nInt(nInt), nStr(nStr), iter(iter), maxkey(maxkey), Cursor(q)
		{
		}
		virtual void Next()
		{
			foundnext = false;
			while (!isEmpty())
			{
				NextItem(data);

				if (q.match(data))
				{
					foundnext = true;
					break;
				}
			}
		}
		virtual bool isEmpty() override
		{
			return iter.isEnd() || iter.Key() > maxkey;
		}
		virtual void NextItem(DummyItem & ret) override
		{
			TValue v = iter.Val();
			ret = to_item(rawdata.Get<char>(v), nInt, nStr);
			iter.Next();
			return;
		}
	};
/*	class BTreePrimaryCursor : public Cursor
	{
		vector<DummyItem> &data;
		vector<DummyItem>::iterator it;
	public:
		BTreePrimaryCursor(vector<DummyItem> &data, DummyQuery q = DummyQuery())
			: data(data), Cursor(q)
		{
			it = data.begin();
		}
		virtual bool isEmpty() override
		{
			return it == data.end();
		}
		virtual DummyItem NextItem() override
		{
			DummyItem ret = *it;
			it++;
			return ret;
		}
	};*/

	virtual unique_ptr<Cursor> cursor(DummyQuery q = DummyQuery()) override
	{
//		auto ret = new BTreePrimaryCursor(data, q);
//		return unique_ptr<Cursor>(ret);
		auto ret = new BTreeCursor<TBTreeIntKey>(rawdata, nInt, nStr, IntKey[0]->LowerBound(INT32_MIN), INT32_MAX, q);
		return unique_ptr<Cursor>(ret);
	}
	virtual unique_ptr<Cursor> cursor(int idx, int low, int high, DummyQuery q = DummyQuery()) override
	{
		auto ret = new BTreeCursor<TBTreeIntKey>(rawdata, nInt, nStr, IntKey[idx]->LowerBound(low), high, q);
		return unique_ptr<Cursor>(ret);
	}
	virtual unique_ptr<Cursor> cursor(int idx, int intkey, DummyQuery q = DummyQuery()) override
	{
		auto ret = new BTreeCursor<TBTreeIntKey>(rawdata, nInt, nStr, IntKey[idx]->LowerBound(intkey), intkey, q);
		return unique_ptr<Cursor>(ret);
	}
	virtual unique_ptr<Cursor> cursor(int idx, string strkey, DummyQuery q = DummyQuery()) override
	{
		size_t h = hash_str(strkey);
		auto ret = new BTreeCursor<TBTreeStrKey>(rawdata, nInt, nStr, StrKey[idx]->LowerBound(h), h, q);
		return unique_ptr<Cursor>(ret);
	}


};
