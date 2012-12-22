#pragma once

#include "includes.h"
#include "dummydb.h"
#include "utils.h"

#ifdef WINDOWS

#include "../db_win64/db_cxx.h"

#else

#include "./db_cxx.h"

#endif

using namespace std;

class BDBItem
{
	int size;
	char *buf;
public:
	BDBItem(DummyItem & item)
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
	Dbt dbt()
	{
		return Dbt(buf, size);
	}
	~BDBItem()
	{
		delete [] buf;
	}
};

class BDBTable : public BaseTable
{
private:
	unique_ptr<Db> PrimaryKey;
	vector<unique_ptr<Db>> IntKey;
	vector<unique_ptr<Db>> StrKey;
	vector<string> DbName;
	size_t totalKeys;

	Db* NewDB(string name, bool isduplicated = false);

	static unique_ptr<DbEnv> dbenv;

public:
	BDBTable(string tablename, int nInt, int nIntKey, int nStr, int nStrKey, vector<int> StringTypeLen) :
		BaseTable(nInt, nIntKey, nStr, nStrKey, StringTypeLen), totalKeys(0)
	{

		tablename = "db_" + tablename;
		PrimaryKey.reset(NewDB(tablename + ".primarykey", false));
		for (int i = 0; i < nIntKey; i++)
		{
			IntKey.emplace_back(NewDB(tablename + ".intkey." + tostring(i), true));
		}
		for (int i = 0; i < nStrKey; i++)
		{
			StrKey.emplace_back(NewDB(tablename + ".strkey." + tostring(i), true));
		}
	}

	virtual ~BDBTable()
	{
		PrimaryKey->close(0);
		for (auto & db : IntKey)
			db->close(0);
		for (auto & db : StrKey)
			db->close(0);
	}


	bool Insert(DummyItem &dummyitem);

	vector<DummyItem> Get();
	vector<DummyItem> Get(DummyQuery& q);
	vector<DummyItem> GetIntKey(int idx, int key);
	vector<DummyItem> GetIntKey(int idx, int key, DummyQuery &q);
	vector<DummyItem> GetIntKeyRange(int idx, int low, int high);
	vector<DummyItem> GetIntKeyRange(int idx, int low, int high, DummyQuery &q);
	vector<DummyItem> GetStrKey(int idx, string str);
	vector<DummyItem> GetStrKey(int idx, string str, DummyQuery& q);
/*
	template <class It>
	class Cursor
	{
		It iter, iter_end;
	public:
		Cursor(It iter, It iter_end) :
			iter(iter), iter_end(iter_end)
		{
		}
		bool HasNext()
		{
			return iter != iter_end;
		}
		void Next()
		{
			iter++;
		}
		auto Data() -> decltype(*iter)
		{
			return *iter;
		}
	};*/
/*

	auto cursor() -> Cursor<vector<DummyItem>::iterator>
	{
		return Cursor<vector<DummyItem>::iterator>(data.begin(), data.end());
	}
	auto cursor(int idx, int low, int high) -> Cursor<multimap<int, DummyItem>::iterator>
	{
		return Cursor<multimap<int, DummyItem>::iterator>(IntKey[idx].lower_bound(low), IntKey[idx].upper_bound(high));
	}
	auto cursor(int idx, int intkey) -> Cursor<multimap<int, DummyItem>::iterator>
	{
		auto range = IntKey[idx].equal_range(intkey);
		return Cursor<multimap<int, DummyItem>::iterator>(range.first, range.second);
	}
	auto cursor(int idx, string str) -> Cursor<unordered_map<string, DummyItem>::iterator>
	{
		auto range = StrKey[idx].equal_range(str);
		return Cursor<unordered_map<string, DummyItem>::iterator>(range.first, range.second);
	}
*/

};

