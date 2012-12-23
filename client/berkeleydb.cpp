#include "berkeleydb.h"

unique_ptr<DbEnv> BDBTable::dbenv;

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

Db* BDBTable::NewDB(string name, bool isduplicated)
{
	if (dbenv.get() == nullptr)
	{
		dbenv.reset(new DbEnv(0));
		dbenv->set_cachesize(0, 512*1024*1024, 1);
		dbenv->open("data/", DB_CREATE | DB_INIT_MPOOL, 0);
	}
	Db* db = new Db(dbenv.get(), 0);
	db->set_pagesize(65536);
	if (isduplicated)
	{
		db->set_flags(DB_DUPSORT);
	}
	DbName.push_back(name);
	db->open(NULL, name.c_str(), NULL, DB_BTREE, DB_CREATE, 0);
	return db;
}


bool BDBTable::Insert(DummyItem &dummyitem)
{
	BDBItem bdbitem(dummyitem);
	Dbt data = bdbitem.dbt();
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
	totalKeys++;
	return true;
}
/*
vector<int> BDBTable::Get()
{
	vector<int> answer;
	Dbc *cursorp;
	PrimaryKey->cursor(NULL, &cursorp, 0);
	Dbt key, data;
	int ret;
	while ((ret = cursorp->get(&key, &data, DB_NEXT)) == 0)
	{
		int k = *(int*)key.get_data();
	//	DummyItem item = to_item(data, nInt, nStr);
		answer.push_back(k);
	}
	cursorp->close();
	return answer;
}

vector<int> BDBTable::Get(DummyQuery& q)
{
	vector<int> answer;
	Dbc *cursorp;
	PrimaryKey->cursor(NULL, &cursorp, 0);
	Dbt key, data;
	int ret;
	while ((ret = cursorp->get(&key, &data, DB_NEXT)) == 0)
	{
		int k = *(int*)key.get_data();
		DummyItem item = to_item(data, nInt, nStr);
		if (q.match(item))
			answer.push_back(k);
	}
	cursorp->close();
	return answer;
}

vector<int> BDBTable::GetIntKey(int idx, int intkey)
{
	vector<int> answer;
	Dbc *cursorp;
	IntKey[idx]->cursor(NULL, &cursorp, 0);
	Dbt key(&intkey, sizeof(int)), data;
	int ret = cursorp->get(&key, &data, DB_SET);
	while (ret != DB_NOTFOUND)
	{
		int k = *(int*)data.get_data();
		answer.push_back(k);
		ret = cursorp->get(&key, &data, DB_NEXT_DUP);
	}
	cursorp->close();
	return answer;
}

vector<int> BDBTable::GetIntKey(int idx, int intkey, DummyQuery& q)
{
	vector<int> answer;
	Dbc *cursorp;
	IntKey[idx]->cursor(NULL, &cursorp, 0);
	Dbt key(&intkey, sizeof(int)), data;
	int ret = cursorp->get(&key, &data, DB_SET);
	while (ret != DB_NOTFOUND)
	{
		int k = *(int*)data.get_data();
		Dbt key2(&k, sizeof(int)), data;
		DummyItem item = to_item(data, nInt, nStr);
		if (q.match(item))
			answer.push_back(k);
		ret = cursorp->get(&key, &data, DB_NEXT_DUP);
	}
	cursorp->close();
	return answer;
}

vector<int> BDBTable::GetIntKeyRange(int idx, int low, int high)
{
	vector<int> answer;
	Dbc *cursorp;
	PrimaryKey->cursor(NULL, &cursorp, 0);
	Dbt key(&low, sizeof(int)), data;
	int ret = cursorp->get(&key, &data, DB_SET_RANGE);
	while (ret != DB_NOTFOUND)
	{
		int k = *(int*)key.get_data();
		if (k > high)
			break;
		DummyItem item = to_item(data, nInt, nStr);
		//if (q.match(item))
			answer.push_back(item);
		ret = cursorp->get(&key, &data, DB_NEXT);
	}
	cursorp->close();
	return answer;
}

vector<int> BDBTable::GetIntKeyRange(int idx, int low, int high, DummyQuery& q)
{
	vector<int> answer;
	Dbc *cursorp;
	PrimaryKey->cursor(NULL, &cursorp, 0);
	Dbt key(&low, sizeof(int)), data;
	int ret = cursorp->get(&key, &data, DB_SET_RANGE);
	while (ret != DB_NOTFOUND)
	{
		int k = *(int*)key.get_data();
		if (k > high)
			break;
		DummyItem item = to_item(data, nInt, nStr);
		if (q.match(item))
			answer.push_back(item);
		ret = cursorp->get(&key, &data, DB_NEXT);
	}
	cursorp->close();
	return answer;
}


vector<int> BDBTable::GetStrKey(int idx, string str)
{
	vector<int> answer;
	Dbc *cursorp;
	PrimaryKey->cursor(NULL, &cursorp, 0);
	Dbt key((char*)str.c_str(), str.length() + 1), data;
	int ret = cursorp->get(&key, &data, DB_SET);
	while (ret != DB_NOTFOUND)
	{
		int k = *(int*)key.get_data();
		DummyItem item = to_item(data, nInt, nStr);
		//if (q.match(item))
			answer.push_back(item);
		ret = cursorp->get(&key, &data, DB_NEXT_DUP);
	}
	cursorp->close();
	return answer;
}

vector<int> BDBTable::GetStrKey(int idx, string str, DummyQuery& q)
{
	vector<int> answer;
	Dbc *cursorp;
	PrimaryKey->cursor(NULL, &cursorp, 0);
	Dbt key((char*)str.c_str(), str.length() + 1), data;
	int ret = cursorp->get(&key, &data, DB_SET);
	while (ret != DB_NOTFOUND)
	{
		int k = *(int*)key.get_data();
		DummyItem item = to_item(data, nInt, nStr);
		//if (q.match(item))
			answer.push_back(item);
		ret = cursorp->get(&key, &data, DB_NEXT_DUP);
	}
	cursorp->close();
	return answer;
}
*/