#ifndef SORTED_H
#define SORTED_H

#include "cs165.h"

// initializes a column index object with the given size
void initializeColumnIndex(ColumnIndex** cindex, size_t size);

// shifts all values within a given range forward, adding "increment" to each value
void shiftValues(int* data, int min, int max, int increment);

// inserts a value into a data array; assumes there's enough space
int insertSorted(int* data, int value, int total);

// inserts a value and an index into a ColumnIndex object
void insertIndex(ColumnIndex* column, int value, int index);

// returns the number of values selected
int findRangeS(int** data, ColumnIndex* column, int total_num, int minimum, int maximum);

#endif
