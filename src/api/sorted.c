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
