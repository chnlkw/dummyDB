#pragma once

#include "includes.h"
#include "dummydb.h"
#include "utils.h"

#include "./db_cxx.h"

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

	virtual ~BDBTable()
	{
		PrimaryKey->close(0);
		for (auto & db : IntKey)
			db->close(0);
		for (auto & db : StrKey)
			db->close(0);
	}

	DummyItem to_item(Dbt &d, int nInt, int nStr)
	{
		DummyItem item;
		int * p = (int*)d.get_data();
		for (int i = 0; i < nInt; i++)
		{
			item.intdata.push_back(*p);
			p++;
		}
		char *s = (char*)p;
		for (int i = 0; i < nStr; i++)
		{
			item.strdata.push_back(s);
			s += strlen(s) + 1;
		}
		return item;
	}
	bool Insert(DummyItem &dummyitem)
	{
/*		data.push_back(item);
		for (int i = 0; i < nIntKey; i++)
			IntKey[i].insert(make_pair(item.intdata[i], item));
		for (int i = 0; i < nStrKey; i++)
			StrKey[i].insert(make_pair(item.strdata[i], item));*/
		BDBItem bdbitem(dummyitem);
		Dbt data = bdbitem.dbt();
		totalKeys++;
		Dbt key(&totalKeys, sizeof(totalKeys));
		int ret = PrimaryKey->put(NULL, &key, &data, 0);
		for (int i = 0; i < nIntKey; i++)
		{
			Dbt index(&dummyitem.intdata[i], sizeof(int));
			IntKey[i]->put(NULL, &index, &key, 0);
		}
		for (int i = 0; i < nStrKey; i++)
		{
			Dbt index((char*)dummyitem.strdata[i].c_str(), dummyitem.strdata[i].length()+1);
			StrKey[i]->put(NULL, &index, &key, 0);
		}
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

