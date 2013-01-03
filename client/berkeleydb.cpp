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

int bt_int_compare(DB *db, const DBT *dbt1, const DBT *dbt2)
{
	int k1 = *(int*)dbt1->data;
	int k2 = *(int*)dbt2->data;
	if (k1 < k2) return -1;
	else if (k1 == k2) return 0;
	else return 1;
}

Db* BDBTable::NewDB(string name, bool isduplicated, bt_compare_fcn_type compare_fun)
{
	if (dbenv.get() == nullptr)
	{
		dbenv.reset(new DbEnv(0));
		dbenv->set_cachesize(0, CACHE_SIZE, 1);
		dbenv->open("data/", DB_CREATE | DB_INIT_MPOOL, 0);
	}
	Db* db = new Db(dbenv.get(), 0);
	db->set_pagesize(65536);
	if (isduplicated)
	{
		db->set_flags(DB_DUPSORT);
	}
	if (compare_fun)
	{
		db->set_bt_compare(compare_fun);
	}
	DbName.push_back(name);
	db->open(NULL, name.c_str(), NULL, DB_BTREE, DB_CREATE, 0);
	return db;
}


bool BDBTable::Insert(DummyItem &dummyitem)
{
	BDBItem bdbitem(dummyitem);
	Dbt data = bdbitem.dbt();

	Dbt key(&totalKeys, sizeof(int));
	int ret = PrimaryKey->put(NULL, &key, &data, 0);
	
	/*for (int i = 0; i < nIntKey; i++)
	{
		Dbt index(&dummyitem.intdata[i], sizeof(int));
		IntKey[i]->put(NULL, &index, &key, 0);
	}*/
	for (int i = 0; i < nStrKey; i++)
	{
		Dbt index((char*)dummyitem.strdata[i].c_str(), dummyitem.strdata[i].length()+1);
		StrKey[i]->put(NULL, &index, &key, 0);
	}
	totalKeys++;
	return true;
}

void BDBTable::UpdateKey()
{
	int n = totalKeys - updatedKeys;
	if (n == 0) return;
	cerr << this->DbName[0] << "  " << n << " to updated " << totalKeys<<' '<<updatedKeys <<endl;
	vector<vector<pair<int, int>>> tmpkey(nIntKey, vector<pair<int, int>>(n));
	cerr << "reading key " << endl;
	for (int keyid = updatedKeys, order = 0; keyid < totalKeys; keyid++, order++)
	{
		auto item = GetData(keyid);
		for (int i = 0; i < nIntKey; i++)
			tmpkey[i][order] = make_pair(item.intdata[i], keyid);
		if (order % 100000 == 0) cerr << "\t" << order << endl;
	}
	for (int i = 0; i < nIntKey; i++)
	{
		cerr << "sorting key " << i << endl;
		sort(tmpkey[i].begin(), tmpkey[i].end());
	}
	for (int i = 0; i < nIntKey; i++)
	{
		cerr << "writing key " << i << endl;
		for (int keyid = updatedKeys, order = 0; keyid < totalKeys; keyid++, order++)
		{
			Dbt key(&tmpkey[i][order].second, sizeof(int));
			Dbt index(&tmpkey[i][order].first, sizeof(int));


			int ret = IntKey[i]->put(NULL, &index, &key, 0);
			if (ret)
					cerr <<"err updatekey ret="<<ret<<endl;
			if (order % 100000 == 0) cerr << "\t" << order << endl;
		}
	}
	/*for (int i = 0; i < nStrKey; i++)
	{
		for (int keyid = updatedKeys; keyid < totalKeys; keyid++)
		{
			auto item = GetData(keyid);
			Dbt key(&keyid, sizeof(int));
			Dbt index((char*)item.strdata[i].c_str(), item.strdata[i].length()+1);
			StrKey[i]->put(NULL, &index, &key, 0);
		}
	}*/

	for (int i = 0; i < 1; i++)
	{
	//	cout << "count int key "<< i << " "<< CountIntKeyRange(i, -10, 100000 - 1) << endl;
		Dbc *cursorp;
		IntKey[i]->cursor(NULL, &cursorp, 0);
		Dbt key, data;
		int ret;
		while ((ret = cursorp->get(&key, &data, DB_NEXT)) == 0)
		{
			int k = *(int*)key.get_data();
		//	DummyItem item = to_item(data, nInt, nStr);
		//	cout << "get "<< *(int*)key.get_data()<< " "<< *(int*)data.get_data()<<endl;
		}
		cursorp->close();
	}
	updatedKeys = totalKeys;
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
