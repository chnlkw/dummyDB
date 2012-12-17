#pragma once

#include "includes.h"
#include "dummydb.h"
#include "utils.h"

#include <db_cxx.h>

using namespace std;



class BDBTable : public BaseTable
{
private:
	unique_ptr<Db> PrimaryKey;
	vector<unique_ptr<Db>> IntKey;
	vector<unique_ptr<Db>> StrKey;
	vector<string> DbName;
	size_t totalKeys;

	Db* NewDB(string name)
	{
		Db* db = new Db(nullptr, 0);
		DbName.push_back(name);
		db->open(NULL, name.c_str(), NULL, DB_BTREE, DB_CREATE, 0);
		return db;
	}

public:
	BDBTable(string tablename, int nInt, int nIntKey, int nStr, int nStrKey, vector<int> StringTypeLen) :
		BaseTable(nInt, nIntKey, nStr, nStrKey, StringTypeLen), totalKeys(0)
	{
		tablename = "db_" + tablename;
		PrimaryKey.reset(NewDB(tablename + ".primarykey"));
		for (int i = 0; i < nIntKey; i++)
		{
			IntKey.emplace_back(NewDB(tablename + ".intkey." + tostring(i)));
		}
		for (int i = 0; i < nStrKey; i++)
		{
			StrKey.emplace_back(NewDB(tablename + ".strkey." + tostring(i)));
		}
	}

	~BDBTable()
	{
		PrimaryKey->close(0);
		for (auto & db : IntKey)
			db->close(0);
		for (auto & db : StrKey)
			db->close(0);
	}
	Dbt to_dbt(DummyItem & item)
	{
		stringstream ss;
		for (auto & i : item.intdata)
			ss.write((char*)&i, sizeof(int));
		for (auto & s : item.strdata)
			ss << s << '\0';
		char * str = &ss.str()[0];
		for (auto & i : item.intdata)
			cerr << i << ',';
		for (auto & s : item.strdata)
			cerr << s <<',';
		cerr << "\tlen = " <<ss.str().length() << endl;
		return Dbt(str, ss.str().length());
	}
	bool Insert(DummyItem &item)
	{
/*		data.push_back(item);
		for (int i = 0; i < nIntKey; i++)
			IntKey[i].insert(make_pair(item.intdata[i], item));
		for (int i = 0; i < nStrKey; i++)
			StrKey[i].insert(make_pair(item.strdata[i], item));*/
		Dbt data = to_dbt(item);
		return true;
	}

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

