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

DummyItem to_item(Dbt &d, int nInt, int nStr);

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

	virtual ~BDBTable() override
	{
		PrimaryKey->close(0);
		for (auto & db : IntKey)
			db->close(0);
		for (auto & db : StrKey)
			db->close(0);
	}

	virtual const int GetDataSize() const override
	{
		return totalKeys;
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

	virtual const DummyItem& GetData(int index) const override
	{
		Dbt key(&index, sizeof(int));
		Dbt data;
		PrimaryKey->get(NULL, &key, &data, 0);
		return to_item(data, nInt, nStr);
	}

	template <typename Function>
	class BdbCursor : public Cursor
	{
	protected:
		Dbc *cursorp;
		Function f;
		Dbt key, data;
		int flagFirst;
		int flagNext;
		int ret;
	public:
		BdbCursor(Dbc *cursorp, Dbt initKey, int flagFirst, int flagNext, Function &&f, DummyQuery q = DummyQuery())
			: cursorp(cursorp), key(initKey), flagFirst(flagFirst), flagNext(flagNext), f(f), Cursor(q)
		{
			ret = cursorp->get(&key, &data, flagFirst);
		}
		virtual bool isEmpty() override
		{
			return ret != 0;
		}
		virtual DummyItem NextItem() override
		{
			assert (ret == 0);
			DummyItem item = f(data);
			ret = cursorp->get(&key, &data, flagNext);
			return item;
		}
	};
	template <typename Function>
	class BdbCursorRange : public BdbCursor<Function>
	{
	private:
		int high;
		bool outrange;
	public:
		BdbCursorRange(Dbc *cursorp, int low, int high, int flagFirst, int flagNext, Function &&f, DummyQuery q = DummyQuery())
			: BdbCursor(cursorp, Dbt(&low, sizeof(int)), flagFirst, flagNext, f, q)
		{
			// first data have been got
			if (ret == 0)
				check();
		}
		virtual bool isEmpty() override
		{
			return ret == 0 || outrange;
		}
		virtual DummyItem NextItem() override
		{
			assert (ret == 0 && !outrange);
			DummyItem item = f(data);
			ret = cursorp->get(&key, &data, flagNext);
			check();
			return item;
		}
		void check()
		{
			outrange = *(int*)data.getdata() > high;
		}
	};
	virtual unique_ptr<Cursor> cursor(DummyQuery q = DummyQuery()) override
	{
		Dbc *cursorp;
		PrimaryKey->cursor(NULL, &cursorp, 0);
		auto fun = [this](Dbt &data){return to_item(data, nInt, nStr);};
		return unique_ptr<Cursor>(new BdbCursor<decltype(fun)>(cursorp, Dbt(), DB_NEXT, DB_NEXT, move(fun), q));
	}
	virtual unique_ptr<Cursor> cursor(int idx, int low, int high, DummyQuery q = DummyQuery()) override
	{
		Dbc *cursorp;
		IntKey[idx]->cursor(NULL, &cursorp, 0);
		Dbt key(&low, sizeof(int)), data;
		auto fun = [this](Dbt &data){
			int k = *(int*)data.get_data();
			return GetData(k);
		};
		return unique_ptr<Cursor>(new BdbCursor<decltype(fun)>(cursorp, key, DB_SET_RANGE, DB_NEXT, move(fun), q));
	}
	virtual unique_ptr<Cursor> cursor(int idx, int intkey, DummyQuery q = DummyQuery()) override
	{
		Dbc *cursorp;
		IntKey[idx]->cursor(NULL, &cursorp, 0);
		Dbt key(&intkey, sizeof(int));
		auto fun = [this](Dbt &data){
			int k = *(int*)data.get_data();
			return GetData(k);
		};
		return unique_ptr<Cursor>(new BdbCursor<decltype(fun)>(cursorp, key, DB_SET, DB_NEXT_DUP, move(fun), q));
	}
	virtual unique_ptr<Cursor> cursor(int idx, string strkey, DummyQuery q = DummyQuery()) override
	{
		Dbc *cursorp;
		StrKey[idx]->cursor(NULL, &cursorp, 0);
		Dbt key((char*)strkey.c_str(), strkey.length() + 1);
		auto fun = [this](Dbt &data){
			int k = *(int*)data.get_data();
			return GetData(k);
		};
		return unique_ptr<Cursor>(new BdbCursor<decltype(fun)>(cursorp, key, DB_SET, DB_NEXT_DUP, move(fun), q));
	}
};

