#ifndef _DATABASE_2012_
#define _DATABASE_2012_

#include <string>
#include <vector>

using namespace std;

/**
 * Type of workload.
 *
 * @return Type of workload.
 * @note You should not implement this routine.
 */
string workload();

/**
 * Create a new table.
 *
 * @param table Name of the table.
 * @param column Name list of the columns.
 * @param type Type list of the columns.
 * @param key Column list of the primary key.
 * @note The primary keys will be unique.
 * @note The primary keys will be given in ascending order.
 * @note The orders of elements of column, type, and key
 *       are very important.
 * @note This routine will be called once for each table.
 */
void create(const string& table, const vector<string>& column,
	const vector<string>& type, const vector<string>& key);

/**
 * Train your system and choose the access methods.
 *
 * @param query List of different types of queries.
 * @param weight List of weights for different types of queries.
 * @note The orders of query and weight are very important.
 * @note All constants in the query list may be changed.
 * @note Sum of all members of weight list will be 100.00.
 * @note This routine will be called only once.
 */
void train(const vector<string>& query, const vector<double>& weight);

/**
 * Load initial data.
 *
 * @param table Name of the table.
 * @param row Some rows in csv format.
 * @note The order of rows is not important.
 * @note All rows in the list belong to the same table.
 * @note This routine may be called multiple times for a table.
 */
void load(const string& table, const vector<string>& row);

/**
 * Preprocessing.
 *
 * @note In this routine, you can re-organize the data,
 *       build the indexes, make some statistics,
 *       and any other thing you need to do.
 * @note This is the last chance you are free of charge.
 *       Any time it takes in the following two routines
 *       (execute() and next()) will be measured and used
 *       against you in the evaluation stage.
 * @note This routine will be called only once.
 */
void preprocess();

/**
 * Execute a query or insertion.
 *
 * @param sql The SQL statement.
 * @note Run time of this routine will be measured.
 * @note This routine may be called many times.
 */
void execute(const string& sql);

/**
 * Get a row from the result set of the last query.
 *
 * @param row The returned row in csv format.
 * @return 1 if a row is returned successfully and 0 otherwise.
 * @note This routine will only be called after SELECT statements
 *       and the driver will try to retrieve all the data
 *       until 0 is returned.
 * @note Run time of this routine will be measured.
 * @note This routine may be called many times.
 */
int next(char *row);

/**
 * Close the system.
 *
 * @note Close the sockets and kill other threads in this routine.
 * @note It is your responsibility to stop the program correctly.
 */
void close();

#endif


