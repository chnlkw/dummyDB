#pragma once

#include "includes.h"
#include "defs.h"

using namespace std;



struct DummyItem
{
	vector<int> intdata;
	vector<string> strdata;
};

struct DummyQuery
{
public:
	struct IntRangeQuery
	{
		int low, high; // close interval [low, high]
	};
	multimap<int, IntRangeQuery> intrange;
	map<int, int> intequal;
	map<int, string> strequal;
	vector<pair<int, pair<int, int>>> colIntEqual;
	vector<pair<int, pair<int, int>>> colIntLess;
	vector<pair<int, pair<int, int>>> colIntGreater;
	vector<pair<int, pair<int, int>>> colStrEqual;
	
	void create(int col, int val) {
		intequal[col] = val;
	}
	void create(int col, int low, int high) {
		IntRangeQuery range;
		range.low = low;
		range.high = high;
		intrange.insert(make_pair(col, range));
	}
	void create(int col, string str) {
		strequal[col] = str;
	}
	void create(int col, int table, int col2, int type) {
		switch(type) {
			case 0:
				colIntEqual.push_back(pair<int, pair<int, int>>(col, pair<int, int>(table, col2)));
				break;
			case 1:
				colIntLess.push_back(pair<int, pair<int, int>>(col, pair<int, int>(table, col2)));
				break;
			case 2:
				colIntGreater.push_back(pair<int, pair<int, int>>(col, pair<int, int>(table, col2)));
				break;
			case 3:
				colStrEqual.push_back(pair<int, pair<int, int>>(col, pair<int, int>(table, col2)));
		}
	}
	void clear() {
	  intrange.clear();
	  intequal.clear();
	  strequal.clear();
	  colIntEqual.clear();
	  colIntLess.clear();
	  colIntGreater.clear();
	  colStrEqual.clear();
	}
	bool match(DummyItem &item)
	{
		for (const auto &it : intequal)
		{
			if (item.intdata[it.first] != it.second)
				return false;
		}
		for (const auto &it : intrange)
		{
			if (item.intdata[it.first] < it.second.low || item.intdata[it.first] > it.second.high)
				return false;
		}
		for (const auto &it : strequal)
		{
			if (item.strdata[it.first] != it.second)
				return false;
		}
		return true;
	}
};

class BaseTable
{
protected:
	int nInt, nIntKey;
	int nStr, nStrKey;
	vector<int> StringTypeLen;
	vector<DummyItem> data;
public:
	BaseTable(int nInt, int nIntKey, int nStr, int nStrKey, vector<int>& StringTypeLen) :
		nInt(nInt), nIntKey(nIntKey), nStr(nStr), nStrKey(nStrKey), StringTypeLen(StringTypeLen)
	{
	}
	virtual ~BaseTable(){};
	virtual bool Insert(DummyItem &item) = 0;
	virtual void UpdateKey() {} ;
	virtual const DummyItem GetData(int index) const = 0;
//	virtual const multimap<int, int>& GetIntKey(int index) = 0;
//	virtual const unordered_multimap<string, int>& GetStrKey(int index) = 0;

	virtual const int GetDataSize() const = 0;
	
	virtual const int Count() { return GetDataSize(); };
	virtual const int CountIntKey(int idx, int key) = 0;
	virtual const int CountIntKeyRange(int idx, int low, int high) = 0;
	virtual const int CountStrKey(int idx, string str) = 0;
	

	class Cursor
	{
	private:
		DummyQuery q;
		DummyItem data;
		virtual bool isEmpty() = 0;
		bool foundnext;
	public:
		Cursor(DummyQuery q = DummyQuery()) : q(q) {}
		virtual ~Cursor() {}
		virtual DummyItem NextItem() = 0;
		void Init()
		{
			Next();
		}
		void Next()
		{
			foundnext = false;
			while (!isEmpty())
			{
				data = NextItem();
				if (q.match(data))
				{
					foundnext = true;
					break;
				}
			}
		}
		const DummyItem & getdata() const
		{
			assert(foundnext);
			return data;
		}
		const bool Empty() const
		{
			return !foundnext;
		}
	};

	virtual unique_ptr<Cursor> cursor(DummyQuery q = DummyQuery()) = 0;
	virtual unique_ptr<Cursor> cursor(int idx, int low, int high, DummyQuery q = DummyQuery()) = 0;
	virtual unique_ptr<Cursor> cursor(int idx, int intkey, DummyQuery q = DummyQuery()) = 0;
	virtual unique_ptr<Cursor> cursor(int idx, string str, DummyQuery q = DummyQuery()) = 0;

};

class DummyTable : public BaseTable
{
public:
	vector<multimap<int, int>> IntKey;
	vector<unordered_multimap<string, int>> StrKey;
	/*DummyTable(int nInt, int nIntKey, int nStr, int nStrKey, vector<int>& StringTypeLen, map<string, int>& intCol, map<string, int>& strCol) :
		nInt(nInt), nIntKey(nIntKey), nStr(nStr), nStrKey(nStrKey), intCol(intCol), strCol(strCol), StringTypeLen(StringTypeLen),
		IntKey(nIntKey), StrKey(nStrKey)
	{
	}*/
	DummyTable(int nInt, int nIntKey, int nStr, int nStrKey, vector<int>& StringTypeLen) :
		BaseTable(nInt, nIntKey, nStr, nStrKey, StringTypeLen)
	{
		// be careful that here, we assume, keys are always at the first place
		for (int i = 0; i < nIntKey; i++) {
			IntKey.push_back(multimap<int, int>());
		}
		for (int i = 0; i < nStrKey; i++) {
			StrKey.push_back(unordered_multimap<string, int>());
		}
	}
	bool Insert(DummyItem &item)
	{
		for (int i = 0; i < nIntKey; i++) {
			IntKey[i].insert(make_pair(item.intdata[i], data.size()));
		}
		for (int i = 0; i < nStrKey; i++)
			StrKey[i].insert(make_pair(item.strdata[i], data.size()));
		data.push_back(item);
		return true;
	}

	virtual const DummyItem GetData(int index) const override;
	virtual const int GetDataSize() const override;
//	virtual const multimap<int, int>& GetIntKey(int index) override;
//	virtual const unordered_multimap<string, int>& GetStrKey(int index) override;

	template <class T>
	const int count_range(T range)
	{
		int ret = 0;
		for (auto it = range.first; it != range.second; it++)
			ret++;
		return ret;
	}

	virtual const int CountIntKey(int idx, int key) override
	{
		return count_range(IntKey[idx].equal_range(key));
	}
	virtual const int CountIntKeyRange(int idx, int low, int high) override
	{
		return count_range(make_pair(IntKey[idx].lower_bound(low), IntKey[idx].upper_bound(high)));
	}
	virtual const int CountStrKey(int idx, string str) override
	{
		return count_range(StrKey[idx].equal_range(str));
	}


	template <typename It, typename Function>
	class DummyCursor : public Cursor
	{
	private:
		It it, ed;
		Function f;
	public:
		DummyCursor(It head, It tail, Function &&f, DummyQuery q = DummyQuery())
			: it(head), ed(tail), f(f), Cursor(q)
		{
		}
		virtual bool isEmpty() override
		{
			return it == ed;
		}
		virtual DummyItem NextItem() override
		{
			DummyItem ret = f(it);
			it++;
			return ret;
		}
	};

	virtual unique_ptr<Cursor> cursor(DummyQuery q = DummyQuery()) override;
	virtual unique_ptr<Cursor> cursor(int idx, int low, int high, DummyQuery q = DummyQuery()) override;
	virtual unique_ptr<Cursor> cursor(int idx, int intkey, DummyQuery q = DummyQuery()) override;
	virtual unique_ptr<Cursor> cursor(int idx, string strkey, DummyQuery q = DummyQuery()) override;


};

class BaseDB
{
public:
	int nTable;
	map<string, unique_ptr<BaseTable>> tables;
	BaseDB()
	{
	}
	virtual int CreateTable(unique_ptr<BaseTable> &table, const string& name)
	{
		tables[name].swap(table);
		return nTable++;
	}
	void updateKeys()
	{
		for (auto & table : tables)
		{
			table.second->UpdateKey();
		}
	}
};

class DummyDB : public BaseDB
{
public:
	DummyDB()
	{
	}
};
