#include "split_csv.h"

void split_csv(const char *buf, vector<string>& token)
{
	char temp[65536];
	int i, j;

	token.clear();
	j = 0;

	for (i = 0; buf[i]; i++) {
		if (buf[i] == ',') {
			temp[j] = '\0';
			token.push_back(temp);
			j = 0;
		} else {
			temp[j++] = buf[i];
		}
	}

	temp[j] = '\0';
	token.push_back(temp);
}


