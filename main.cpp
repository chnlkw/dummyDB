
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <iostream>
#include <vector>
#include <cassert>
#include <ctime>

#include "./hash.h"
#include "./client.h"

#define PATH		"test/"
#define SCHEMA		(PATH "schema")
#define STATISTIC	(PATH "statistic")
#define QUERY		(PATH "query")

using namespace std;

//#include "db_win64\db.h"


#include "client/buffer.h"

int main(int argc, char **argv)
{
	//DB *dbp;
	//int ret = db_create(&dbp, NULL, 0);
	FILE *fin;
	char* buf = new char[65536];
	char *buf2 = new char[65536];
	int i, j, cnt, cnt2, res;
	string table;
	vector<string> tables, column, type;
	vector<string> key, query, row;
	vector<double> weight;
	clock_t start, end;

	printf("Benchmark: %s\n", workload().c_str());

	/* Schema */

	fin = fopen(SCHEMA, "r");
	assert(fin != NULL);

	res = fscanf(fin, "%d", &cnt);
	assert(res == 1);
	for (i = 0; i < cnt; i++) {
		res = fscanf(fin, "%s", buf);
		assert(res == 1);
		table = buf;
		tables.push_back(table);

		column.clear();
		type.clear();
		res = fscanf(fin, "%d", &cnt2);
		assert(res == 1);
		for (j = 0; j < cnt2; j++) {
			res = fscanf(fin, "%s %s", buf, buf2);
			assert(res == 2);
			column.push_back(buf);
			type.push_back(buf2);
		}

		key.clear();
		res = fscanf(fin, "%d", &cnt2);
		assert(res == 1);
		for (j = 0; j < cnt2; j++) {
			res = fscanf(fin, "%s", buf);
			assert(res == 1);
			key.push_back(buf);
		}

		create(table, column, type, key);
	}

	fclose(fin);

	/* Statistic */

	fin = fopen(STATISTIC, "r");
	assert(fin != NULL);

	query.clear();
	weight.clear();
	res = fscanf(fin, "%d", &cnt);
	assert(res == 1);
	for (i = 0; i < cnt; i++) {
restart_1:
		char *str = fgets(buf, 65536, fin);
		assert(str == buf);
		int len = strlen(buf);
		if (len > 0 && buf[len - 1] == '\n') {
			buf[len - 1] = '\0';
			len--;
		}
		if (len == 0)
			goto restart_1;
		query.push_back(buf);

		double temp;
		res = fscanf(fin, "%lf", &temp);
		assert(res == 1);
		weight.push_back(temp);
	}

	train(query, weight);

	fclose(fin);

	/* Load initial data */
	if (argc == 2)
	{
		fprintf(stderr, "skip load table\n");
	}else
	for (i = 0; i < tables.size(); i++) {

		std::cerr << "Load table " << i << std::endl;

		sprintf(buf, PATH "%s.data", tables[i].c_str());
		fin = fopen(buf, "r");
		assert(fin != NULL);

		row.clear();
		res = fscanf(fin, "%d", &cnt);
		assert(res == 1);

		size_t lines = 0;

		for (j = 0; j < cnt; j++) {
			res = fscanf(fin, "%s", buf);
			assert(res == 1);
			row.push_back(buf);
			lines ++;
			if (row.size() == 65536) {
				cerr << "loaded lines " << lines << endl;
				load(tables[i], row);
				cerr << "Inserted " << lines << endl;
				row.clear();
			}
		}
		if (row.size() > 0)
			load(tables[i], row);

		fclose(fin);
	}

	/* Preprocessing */

	preprocess();

	/* Execute queries */

	start = clock();

	fin = fopen(QUERY, "r");
	assert(fin != NULL);

	res = fscanf(fin, "%d", &cnt);
	assert(res == 1);
	for (i = 0; i < cnt; i++) {
restart_2:
		char *str = fgets(buf, 65536, fin);
		assert(str == buf);
		int len = strlen(buf);
		if (len > 0 && buf[len - 1] == '\n') {
			buf[len - 1] = '\0';
			len--;
		}
		if (len == 0)
			goto restart_2;

		execute(buf);

		if (strstr(buf, "INSERT") != NULL)
			continue;

		unsigned int checksum = 0;
		while (next(buf2))
			checksum += myhash(buf2);

		printf("Checksum: %X\n--------\n", checksum);
		//fprintf(stderr, "Checksum: %X\n--------\n", checksum);
		pcache->Print();
	}

	fclose(fin);

	end = clock();

	double interval = (double) (end - start) / CLOCKS_PER_SEC;

	printf("Response time: %.3lf sec\n", interval);

	/* Close */

	close();

	system("pause");

	return (0);
}


