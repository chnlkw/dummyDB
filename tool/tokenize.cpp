#include "tokenize.h"

void tokenize(const char *buf, vector<string>& token)
{
	char temp[65536];
	int i, j;

	token.clear();

	for (i = 0; buf[i]; i++) {
		// Space
		if (buf[i] == ' ' ||
			buf[i] == '\t' ||
			buf[i] == '\r' ||
			buf[i] == '\n')
			continue;

		// Identifier
		if (buf[i] == '_' ||
			'a' <= buf[i] && buf[i] <= 'z' ||
			'A' <= buf[i] && buf[i] <= 'Z') {
			j = 0;
			while (buf[i] == '_' ||
				'a' <= buf[i] && buf[i] <= 'z' ||
				'A' <= buf[i] && buf[i] <= 'Z' ||
				'0' <= buf[i] && buf[i] <= '9') {
				temp[j++] = buf[i++];
			}
			temp[j] = '\0';
			token.push_back(temp);
			i--;
			continue;
		}

		// Number
		if ('0' <= buf[i] && buf[i] <= '9') {
			j = 0;
			while ('0' <= buf[i] && buf[i] <= '9') {
				temp[j++] = buf[i++];
			}
			temp[j] = '\0';
			token.push_back(temp);
			i--;
			continue;
		}

		// String
		if (buf[i] == '\'') {
			j = 0;
			while (1) {
				temp[j++] = buf[i++];
				if (j > 1 && temp[j - 1] == '\'')
					break;
			}
			temp[j] = '\0';
			token.push_back(temp);
			i--;
			continue;
		}

		// < = > , ;
		if (buf[i] == '=' || buf[i] == '<' || buf[i] == '>' ||
			buf[i] == '(' || buf[i] == ')' || buf[i] == ',' || buf[i] == ';') {
			temp[0] = buf[i];
			temp[1] = '\0';
			token.push_back(temp);
			continue;
		}

		fprintf(stderr, "ERROR: This code should never be executed.\n");
		exit(1);
	}
}


