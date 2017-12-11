//
// Created by Sam Christian on 12/5/17.
//

#ifndef PROG3_SORTER_SERVER_H
#define PROG3_SORTER_SERVER_H
typedef struct colType {
    char *name;
    char *type;
} column;

typedef struct colEntryType {
    char *name;
    char *value;
} colEntry;

typedef struct rowType {
    colEntry colEntries[28];
} rowType;
#endif //PROG3_SORTER_SERVER_H
