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
public:
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
	virtual const DummyItem& GetData(int index) const = 0;

	/*
	virtual vector<int> Get() = 0;
	virtual vector<int> Get(DummyQuery& q) = 0;
	virtual vector<int> GetIntKey(int idx, int key) = 0;
	virtual vector<int> GetIntKey(int idx, int key, DummyQuery &q) = 0;
	virtual vector<int> GetIntKeyRange(int idx, int low, int high) = 0;
	virtual vector<int> GetIntKeyRange(int idx, int low, int high, DummyQuery &q) = 0;
	virtual vector<int> GetStrKey(int idx, string str) = 0;
	virtual vector<int> GetStrKey(int idx, string str, DummyQuery &q) = 0;
	*/

	class Cursor
	{
	private:
		DummyQuery q;
		DummyItem data;
	public:
		Cursor(DummyQuery q = DummyQuery()) : q(q) {}
		~Cursor() {}
		virtual bool HasNext() { return false; }
		virtual DummyItem && NextItem() { return DummyItem(); }
		void Next()
		{
			while (HasNext())
			{
				data = NextItem();
				if (q.match(data))
					break;
			}
		}
		DummyItem & getdata()
		{
			return data;
		}
	};

	virtual Cursor cursor(DummyQuery q = DummyQuery()) = 0;
	virtual Cursor cursor(int idx, int low, int high, DummyQuery q = DummyQuery()) = 0;
	virtual Cursor cursor(int idx, int intkey, DummyQuery q = DummyQuery()) = 0;
	virtual Cursor cursor(int idx, string str, DummyQuery q = DummyQuery()) = 0;

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

	virtual const DummyItem& GetData(int index) const override;
	/*vector<int> Get();
	vector<int> Get(DummyQuery& q);
	vector<int> GetIntKey(int idx, int key);
	vector<int> GetIntKey(int idx, int key, DummyQuery &q);
	vector<int> GetIntKeyRange(int idx, int low, int high);
	vector<int> GetIntKeyRange(int idx, int low, int high, DummyQuery &q);
	vector<int> GetStrKey(int idx, string str);
	vector<int> GetStrKey(int idx, string str, DummyQuery& q);*/

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
		bool HasNext()
		{
			return it != ed;
		}
		DummyItem && NextItem()
		{
			DummyItem ret = f(it);
			it++;
			return move(ret);
		}
	};

	virtual Cursor cursor(DummyQuery q = DummyQuery()) override
	{
		typedef decltype(data.begin()) It;
		auto fun = [](It it) { return *it; };
		return DummyCursor<It, decltype(fun)>(data.begin(), data.end(), move(fun), q);
	}
	virtual Cursor cursor(int idx, int low, int high, DummyQuery q = DummyQuery()) override
	{
		typedef decltype(IntKey[idx].begin()) It;
		auto fun = [this](It it) { return data[it->second]; };
		return DummyCursor<It, decltype(fun)>(IntKey[idx].lower_bound(low), IntKey[idx].upper_bound(high), move(fun), q);
	}
	virtual Cursor cursor(int idx, int intkey, DummyQuery q = DummyQuery()) override
	{
		typedef decltype(IntKey[idx].begin()) It;
		auto fun = [this](It it) { return data[it->second]; };
		return DummyCursor<It, decltype(fun)>(IntKey[idx].lower_bound(intkey), IntKey[idx].upper_bound(intkey), move(fun), q);
	}
	virtual Cursor cursor(int idx, string strkey, DummyQuery q = DummyQuery()) override
	{
		typedef decltype(StrKey[idx].begin()) It;
		auto fun = [this](It it) { return data[it->second]; };
		return DummyCursor<It, decltype(fun)>(StrKey[idx].lower_bound(strkey), StrKey[idx].upper_bound(strkey), move(fun), q);
	}

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
};

class DummyDB : public BaseDB
{
public:
	DummyDB()
	{
	}
};
