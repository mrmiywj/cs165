#include "api/sorted.h"

void initializeColumnIndex(ColumnIndex** cindex, size_t size) {
    ColumnIndex* new_index = malloc(sizeof(ColumnIndex));
    new_index->values = calloc(1, size);
    new_index->indexes = calloc(1, size);
    *cindex = new_index;
}

void shiftValues(int* data, int min, int max, int increment) {
    for (int i = max; i >= min && i <= max; i--)
        data[i + 1] = data[i] + increment;
}

// inserts a value into a data array; assumes there's enough space
int insertSorted(int* data, int value, int total) {
    // binary search for lowest value greater than this value
    int low = 0;
    int high = total;
    while (high > low) {
        int current = (low + high) / 2;
        if (data[current] < value)
            low = current + 1;
        else
            high = current;
    }
    // low and high now point to the smallest element greater than "value"
    shiftValues(data, low, total, 0);
    data[low] = value;
    return low;
}

// inserts a value and an index into a ColumnIndex object
void insertIndex(ColumnIndex* column, int value, int index) {
    int insert_index = insertSorted(column->values, value, index);
    shiftValues(column->indexes, insert_index, index - 1, 0);
    column->indexes[insert_index] = index;
}

// returns the number of values selected
int findRangeS(int** data, ColumnIndex* column, int total_num, int minimum, int maximum) {
    int capacity = 0;
    int num_tuples = 0;
    int* results = NULL;

    // find smallest value >= minimum
    int low = 0;
    int high = total_num;
    while (high > low) {
        int current = (low + high) / 2;
        if (column->values[current] < minimum)
            low = current + 1;
        else
            high = current;
    }

    // low and high now contain the index of the smallest value >= minimum
    int current = low;
    while (current < total_num && column->values[current] < maximum) {
        // check if we need to resize
        if (capacity == num_tuples) {
            size_t new_size = (capacity == 0) ? 1 : 2 * capacity;
            if (new_size == 1) {
                results = malloc(sizeof(int));
            } else {
                int* new_data = realloc(results, sizeof(int) * new_size);
                if (new_data != NULL) {
                    results = new_data;
                } else {
                    return 0;
                }
            }
            capacity = new_size;
        }
        // save new value
        results[num_tuples++] = column->indexes[current++];
    }

    *data = results;
    return num_tuples;
}
