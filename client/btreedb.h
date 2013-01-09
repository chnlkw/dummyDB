#include "btree.h"

#include "dummydb.h"

hash<string> hash_str;

class BTreeTable : public BaseTable
{	
	typedef BPlusTree<IndexBlockSize, int, uint32_t> TBTreeIntKey;
	typedef BPlusTree<IndexBlockSize, size_t, uint32_t> TBTreeStrKey;
public:
	vector<TBTreeIntKey> IntKey;
	vector<TBTreeStrKey> StrKey;

	BTreeTable(int nInt, int nIntKey, int nStr, int nStrKey, vector<int>& StringTypeLen) :
		BaseTable(nInt, nIntKey, nStr, nStrKey, StringTypeLen)
	{
		for (int i = 0; i < nIntKey; i++) {
			IntKey.emplace_back();
		}
		for (int i = 0; i < nStrKey; i++) {
			StrKey.emplace_back();
		}
	}
	bool Insert(DummyItem &item)
	{
		for (int i = 0; i < nIntKey; i++) {
			IntKey[i].Insert(item.intdata[i], data.size());
		}
		for (int i = 0; i < nStrKey; i++)
			StrKey[i].Insert(hash_str(item.strdata[i]), data.size());
		data.push_back(item);
		return true;
	}

	virtual const DummyItem GetData(int index) const override
	{
		return data[index];
	}
	virtual const int GetDataSize() const override
	{
		return data.size();
	}
//	virtual const multimap<int, int>& GetIntKey(int index) override;
//	virtual const unordered_multimap<string, int>& GetStrKey(int index) override;
	template <typename TT>
	class BTreeCursor : public Cursor
	{
		typedef typename TT::Tkey TKey;
		typedef typename TT::TValue TValue;
		typedef typename TT::Iterator TIter;

		TIter iter;
		TKey maxkey;
		vector<DummyItem> &data;
	public:
		BTreeCursor(vector<DummyItem> &data, TIter &&iter, TKey maxkey, DummyQuery q = DummyQuery())
			: data(data), iter(iter), maxkey(maxkey), Cursor(q)
		{
		}
		virtual bool isEmpty() override
		{
			return iter.isEnd() || iter.Key() > maxkey;
		}
		virtual DummyItem NextItem() override
		{
			TValue v = iter.Val();
			DummyItem ret = data[v];
			iter.Next();
			return ret;
		}
	};
	class BTreePrimaryCursor : public Cursor
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
	};

	virtual unique_ptr<Cursor> cursor(DummyQuery q = DummyQuery()) override
	{
		auto ret = new BTreePrimaryCursor(data, q);
		return unique_ptr<Cursor>(ret);
	}
	virtual unique_ptr<Cursor> cursor(int idx, int low, int high, DummyQuery q = DummyQuery()) override
	{
		auto ret = new BTreeCursor<TBTreeIntKey>(data, IntKey[idx].LowerBound(low), high, q);
		return unique_ptr<Cursor>(ret);
	}
	virtual unique_ptr<Cursor> cursor(int idx, int intkey, DummyQuery q = DummyQuery()) override
	{
		auto ret = new BTreeCursor<TBTreeIntKey>(data, IntKey[idx].LowerBound(intkey), intkey, q);
		return unique_ptr<Cursor>(ret);
	}
	virtual unique_ptr<Cursor> cursor(int idx, string strkey, DummyQuery q = DummyQuery()) override
	{
		size_t h = hash_str(strkey);
		auto ret = new BTreeCursor<TBTreeStrKey>(data, StrKey[idx].LowerBound(h), h, q);
		return unique_ptr<Cursor>(ret);
	}


};