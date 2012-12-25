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
	int totalKeys;

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

	virtual const int Count() override
	{
		return totalKeys;
	}
	virtual const int CountIntKey(int idx, int key) override
	{
		Dbt dbt(&key, sizeof(int));
		DB_KEY_RANGE keyrange;
		IntKey[idx]->key_range(NULL, &dbt, &keyrange, 0);
		int ret = (int) (totalKeys * keyrange.equal);
		return ret;
	}
	virtual const int CountIntKeyRange(int idx, int low, int high) override
	{
		Dbt dbt(&low, sizeof(int));
		DB_KEY_RANGE keyrange;
		IntKey[idx]->key_range(NULL, &dbt, &keyrange, 0);
		int c1 = (int) (totalKeys * keyrange.less);
		dbt.set_data(&high);
		IntKey[idx]->key_range(NULL, &dbt, &keyrange, 0);
		int c2 = (int) (totalKeys * keyrange.equal);
		int c3 = (int) (totalKeys * keyrange.less);
		int ret = c2 + c3 - c1;
		return ret;
	}
	virtual const int CountStrKey(int idx, string str) override
	{
		Dbt dbt((char*)str.c_str(), str.length() + 1);
		DB_KEY_RANGE keyrange;
		StrKey[idx]->key_range(NULL, &dbt, &keyrange, 0);
		int ret = (int) (totalKeys * keyrange.equal);
		return ret;
	}


	virtual const DummyItem GetData(int index) const override
	{
		//cout << "getdata index = " << index << endl;
		Dbt key(&index, sizeof(int));
		char buf[1024];
		Dbt data;
		int ret = PrimaryKey->get(NULL, &key, &data, 0);
		assert(ret == 0);
		//cout << "data = "<< data.get_data() << endl;
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
	class BdbCursorRange : public Cursor
	{
	private:
		Dbc *cursorp;
		Function f;
		Dbt key, data;
		int flagFirst;
		int flagNext;
		int ret;		
		int high;
		bool outrange;
	public:
		BdbCursorRange(Dbc *cursorp, int low, int high, int flagFirst, int flagNext, Function &&f, DummyQuery q = DummyQuery())
			: cursorp(cursorp), key(&low, sizeof(int)), high(high), flagFirst(flagFirst), flagNext(flagNext), f(f), Cursor(q)
		{
			ret = cursorp->get(&key, &data, flagFirst);
			//cout << "ret = " << ret << endl;
			if (ret == 0)
				check();
		}
		virtual bool isEmpty() override
		{ 
		//	cout << "isempty ret = " << ret << endl;
			return ret || outrange;
		}
		virtual DummyItem NextItem() override
		{
			//cout << (ret) << " "<< (outrange) << endl;
		//	cout << "next ret = " << ret << endl;
			assert (ret == 0 && !outrange);
			DummyItem item = f(data);
			ret = cursorp->get(&key, &data, flagNext);
			check();
			//cout << (ret) << "+"<< (outrange) << endl;
		//	cout << "new ret = " << ret << endl;
			return item;
		}
		void check()
		{
			outrange = *(int*)data.get_data() > high;
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
		//cout << "cursor range " << low << ' ' << high << endl;
		Dbc *cursorp;
		IntKey[idx]->cursor(NULL, &cursorp, 0);
		auto fun = [this](Dbt &data){
			int k = *(int*)data.get_data();
		//	cout << "find pri key " << k << endl;
			return GetData(k);
		};
		return unique_ptr<Cursor>(new BdbCursorRange<decltype(fun)>(cursorp, low, high, DB_SET_RANGE, DB_NEXT, move(fun), q));
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

