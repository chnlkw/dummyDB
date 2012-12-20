#include "berkeleydb.h"

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
	Db* db = new Db(nullptr, 0);
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

vector<DummyItem> BDBTable::Get()
{
	vector<DummyItem> answer;
	Dbc *cursorp;
	PrimaryKey->cursor(NULL, &cursorp, 0);
	Dbt key, data;
	int ret;
	while ((ret = cursorp->get(&key, &data, DB_NEXT)) == 0)
	{
		int k = *(int*)key.get_data();
		DummyItem item = to_item(data, nInt, nStr);
		answer.push_back(item);
	}
	cursorp->close();
	return answer;
}

vector<DummyItem> BDBTable::Get(DummyQuery& q)
{
	vector<DummyItem> answer;
	Dbc *cursorp;
	PrimaryKey->cursor(NULL, &cursorp, 0);
	Dbt key, data;
	int ret;
	while ((ret = cursorp->get(&key, &data, DB_NEXT)) == 0)
	{
		int k = *(int*)key.get_data();
		DummyItem item = to_item(data, nInt, nStr);
		if (q.match(item))
			answer.push_back(item);
	}
	cursorp->close();
	return answer;
}

vector<DummyItem> BDBTable::GetIntKey(int idx, int intkey)
{
	vector<DummyItem> answer;
	Dbc *cursorp;
	PrimaryKey->cursor(NULL, &cursorp, 0);
	Dbt key(&intkey, sizeof(int)), data;
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

vector<DummyItem> BDBTable::GetIntKey(int idx, int intkey, DummyQuery& q)
{
	vector<DummyItem> answer;
	Dbc *cursorp;
	PrimaryKey->cursor(NULL, &cursorp, 0);
	Dbt key(&intkey, sizeof(int)), data;
	int ret = cursorp->get(&key, &data, DB_SET);
	while (ret != DB_NOTFOUND)
	{
		int k = *(int*)key.get_data();
		DummyItem item = to_item(data, nInt, nStr);
		if (q.match(item))
			answer.push_back(item);
		ret = cursorp->get(&key, &data, DB_NEXT_DUP);
	}
	cursorp->close();
	return answer;
}

vector<DummyItem> BDBTable::GetIntKeyRange(int idx, int low, int high)
{
	vector<DummyItem> answer;
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

vector<DummyItem> BDBTable::GetIntKeyRange(int idx, int low, int high, DummyQuery& q)
{
	vector<DummyItem> answer;
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


vector<DummyItem> BDBTable::GetStrKey(int idx, string str)
{
	vector<DummyItem> answer;
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

vector<DummyItem> BDBTable::GetStrKey(int idx, string str, DummyQuery& q)
{
	vector<DummyItem> answer;
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
