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
			IntKey.emplace_back(NewDB(tablename + ".intkey." + to_string(i), true));
		}
		for (int i = 0; i < nStrKey; i++)
		{
			StrKey.emplace_back(NewDB(tablename + ".strkey." + to_string(i), true));
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
	/*
	vector<int> Get();
	vector<int> Get(DummyQuery& q);
	vector<int> GetIntKey(int idx, int key);
	vector<int> GetIntKey(int idx, int key, DummyQuery &q);
	vector<int> GetIntKeyRange(int idx, int low, int high);
	vector<int> GetIntKeyRange(int idx, int low, int high, DummyQuery &q);
	vector<int> GetStrKey(int idx, string str);
	vector<int> GetStrKey(int idx, string str, DummyQuery& q);*/


};

