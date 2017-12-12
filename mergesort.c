//
//  mergesort.c
//  systprog1
//
//  Created by Sam Christian on 9/27/17.
//  Copyright © 2017 Sam Christian. All rights reserved.
//

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "sorter_server.h"

void mergeSort(char** rows, int l, int r);
void merge(rowType ** rows, int l, int m, int r);
char *colType;
int colIdx;
long doCompare(rowType *row1, rowType *row2);
int doSort(rowType ** rows, int sortColIdx, char * sortColType, int arrSize){
    colIdx=sortColIdx;
    colType=sortColType;
    
    mergeSort(rows, 0, arrSize-1);
    
    return 0;
}

void mergeSort(char** rows, int l, int r){
    if (l<r){
        
        int m=(l+(r-1))/2;
        
        mergeSort(rows, l, m);
        
        mergeSort(rows, m+1, r);
        
        merge(rows, l, m, r);
    }
    
    
}
void merge(rowType **row, int left, int mid, int right)
{
    int i=0, j=0, k=0;
   
    int n1 = mid - left + 1;
    int n2 =  right - mid;
    
    
    rowType *L[n1], *R[n2];
    
    
    for (i = 0; i < n1; i++)
        L[i] = row[left + i];
    for (j = 0; j < n2; j++)
        R[j] = row[mid + 1+ j];
    
    
  i = 0;
  j = 0;
  k = left;
    while (i < n1 && j < n2)
    {
       
        if (doCompare(L[i], R[j]) <= 0)
        {
            row[k] = L[i];
            i++;
        }
        else
        {
            row[k] = R[j];
            j++;
        }
        k++;
    }
    
       while (i < n1)
    {
        row[k] = L[i];
        i++;
        k++;
    }
    
    
    while (j < n2)
    {
        row[k] = R[j];
        j++;
        k++;
    }
    
}


long doCompare(rowType *row1, rowType *row2)
{
    const char* r1Value = row1->colEntries[colIdx].value;
    const char* r2Value = row2->colEntries[colIdx].value;
    
    if (strcmp(colType, "char") == 0) {
        // if the value starts with a quote advancee to the next character
        if (*r1Value == '"') {
            r1Value++;
        }
        if (*r2Value == '"') {
            r2Value++;
        }
        return(strcmp(r1Value, r2Value));
    }
    else if (strcmp(colType, "int") == 0){
                int i1Value = atoi(r1Value);
                int i2Value = atoi(r2Value);
                return (i1Value - i2Value);
    } else if (strcmp(colType, "long") == 0) {
                long l1Value = atol(r1Value);
                long l2Value = atol(r2Value);
                return (l1Value - l2Value);
    }
    else if (strcmp(colType, "float") == 0) {
                float f1Value = atof(r1Value);
                float f2Value = atof(r2Value);
                return (f1Value - f2Value);
    } else {
        fprintf(stderr, "Unknown colType: %s\n", colType);
        exit(1);
    }
}
