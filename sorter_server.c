#include <stdio.h>
#include <sys/socket.h>
#include <stdlib.h>
#include <netinet/in.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <arpa/inet.h>
#include "sorter_server.h"
#define  SOD "SOD"
#define  EOD "EOD"
#define SOC "SOC"
#define EOC "EOC"
#define DMP "DMP"
#define SRT "SRT"

#define MAX_THREADS 4096
#define BUFF_SIZE 8192
#define MSG_SIZE 4096

        char * readHeader(char *buffer);
#define PORT 8090

        typedef struct headerType {
            char * name;
            char* value;
        } headerType;

        typedef struct allRowsType {
            int32_t rowCount;
            rowType ** rows;
        } allRowsType;

        int allRowsArraySize = 6000;

pthread_mutex_t rowLock;
pthread_mutex_t headerLock;

allRowsType allRows;
column colTypes[29];
int sortColIdx;

void * processClient(void * sockFd);
char** split(char* line, char delimiter);
char * getSortCol(char *buf, int buffSize);
void doTrim(char input[], size_t size);
void setColumns(column colTypes[]);
extern void doSort(rowType **, int, char *, int);
char * getColType(column *, const char *);
int getColIndex(char *column);
void printRows(rowType **rows, int rowCount, int sockFd);
int sorter2(char *lines[], int totalLines, char *sortColumn);
int doSend(int sockFd, char *msg);
int doRead(int sockFd, char *buffer);

char *header;

int main(int argc, char **argv) {
    int server_fd, new_sockets[MAX_THREADS], valread;
    pthread_t tids[MAX_THREADS];
    struct sockaddr_in address;
    int opt = 1;
    int addrlen = sizeof(address);

    const char *usageMsg = "Usage: sorter_server -p<port>";
    int threadCount = 0;
    int port = -1;
    int c;
    while ((c = getopt (argc, argv, "p:")) != -1)
        switch (c)
        {

            case 'p':
                port = atoi(optarg);
                break;

            case '?':
                fprintf (stderr,
                         "Unknown option character `\\x%x'.\n",
                         optopt);
                return 1;
            default:
                abort ();
        }

    if (port == -1) {
        fprintf(stderr, "%s\n", usageMsg);
        exit(EXIT_FAILURE);
    }
    // Creating socket file descriptor
    if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) < 0)
    {
        perror("socket failed");
        exit(EXIT_FAILURE);
    }

    // Set options
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR ,
                   &opt, sizeof(opt)))
    {
        perror("setsockopt");
        exit(EXIT_FAILURE);
    }
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons( port );

    // Bind socket to the port
    if (bind(server_fd, (struct sockaddr *)&address,
             sizeof(address))<0)
    {
        perror("bind failed");
        exit(EXIT_FAILURE);
    }
    if (listen(server_fd, 100) < 0)
    {
        perror("listen");
        exit(EXIT_FAILURE);
    }

    if (pthread_mutex_init(&rowLock, NULL) != 0)
    {

        perror( "Failed to initialize rowLock mutex:");

        exit(EXIT_FAILURE);
    }

    if (pthread_mutex_init(&headerLock, NULL) != 0)
    {

        perror( "Failed to initialize idxLock mutex:");

        exit(EXIT_FAILURE);
    }

    setColumns(colTypes);

    int shutdown = 0;
    int clientCount = 0;
    while(! shutdown) {
        if ((new_sockets[threadCount] = accept(server_fd, (struct sockaddr *) &address,
                                               (socklen_t *) &addrlen)) < 0) {
            perror("accept");
            exit(EXIT_FAILURE);
        }

        if (clientCount == 0) {
            printf("Received connections from: ");
            clientCount++;
        }
        else {
            printf(",");
        }
        printf("%s", inet_ntoa(address.sin_addr));
        pthread_create(&tids[threadCount], NULL, processClient, &new_sockets[threadCount]);

        threadCount++;

        if (threadCount > MAX_THREADS) {
            fprintf(stderr, "Max thread count of %d exceeded\n", MAX_THREADS);
            shutdown = 1;
        }
    }


    exit(0);

}

void *processClient(void *fd) {
    char buffer[MSG_SIZE] = {0};
    int bytesRead;
    int state = 0;

    long buffSize = BUFF_SIZE;

    char **rows = calloc(BUFF_SIZE, sizeof(char *));
    char *sortColumn;
    char *command = SRT;
    int msgLen;
    int sockFd = *(int *) fd;
    char last3[4] = {0};
    long offset = 0;
    int lineCount = 0;
    while (state < 3) {

        // read the message length
//        bytesRead = read(sockFd, &msgLen, sizeof(msgLen));


        // read the data
        memset(buffer, '\0', sizeof(buffer));
//        bytesRead = read(sockFd, buffer, msgLen);

        bytesRead = doRead(sockFd, buffer);
        if (bytesRead == 0) {
            printf("Exiting after 0 bytes read\n");
            break;
        }

//        printf("line: %d read %d bytes %s\n\n", lineCount, bytesRead, buffer);
        doTrim(buffer, bytesRead);
        switch (state) {

            case 0:     // read data

                if (bytesRead == 3 && strncmp(buffer, EOD, 3) == 0) {

//                    printf("got EOD\n");
                    fflush(stdout);
                    state = 1;
                } else if (bytesRead == 3 && strncmp(buffer, DMP, 3) == 0) {
//                    printf("Got DMP sortColumn\n");
                    fflush(stdout);
                    sortColumn = calloc(bytesRead+1, sizeof(char *));
                    memcpy(sortColumn, buffer, bytesRead);
                    state = 3;
                } else {    // data line
                    if (lineCount == 0 && header == NULL) {
                        /*
                         * Store the header for final all sorted response
                         */
                        pthread_mutex_lock(&headerLock);
                        header = calloc(bytesRead+1, sizeof(char *));
                        memcpy(header, buffer, bytesRead);
                        pthread_mutex_unlock(&headerLock);
                    }
                    char *line = calloc(bytesRead + 1, sizeof(char *));
                    memcpy(line, buffer, bytesRead);

                    if (lineCount == buffSize) {
                        buffSize *= 2;
                        rows = realloc(rows, buffSize * sizeof(char *));
//                        printf("realloc !! buffsize is now %d\n", buffSize);


                    }
                    rows[lineCount] = line;

                    lineCount++;
                }
                break;
            case 1 :    // get sort column
//                printf("sortColumn is: %s\n", buffer);
                sortColumn = calloc(bytesRead+1, sizeof(char *));
                memcpy(sortColumn, buffer, bytesRead);
                if (sortColIdx == 0) {
                    pthread_mutex_lock(&headerLock);

                    sortColIdx = getColIndex(sortColumn);
                    pthread_mutex_unlock(&headerLock);
                }

                state = 2;
                break;
            case 2:     // get EOC marker
                if (bytesRead == 3 && strncmp(buffer, EOC, 3) == 0) {

//                    printf("got EOC\n");
                    fflush(stdout);
                    state = 3;
                }
                break;
            default:
                fprintf(stderr, "Unknown state %d input %s\n", state, buffer);
                break;

        }
    }


    if (sortColumn != NULL) {
        if (strcmp(sortColumn, DMP) == 0) {

            pthread_mutex_lock(&rowLock);

            doSort(allRows.rows, sortColIdx, colTypes[sortColIdx].type, allRows.rowCount);

            doSend(sockFd, header);

            printRows(allRows.rows, allRows.rowCount, sockFd);
            pthread_mutex_unlock(&rowLock);

            doSend(sockFd, EOD);


        }

        else {
            int rc = sorter2(rows, lineCount, sortColumn);

            doSend(sockFd, EOC);
        }

    }

    free(rows);
    free(sortColumn);
    pthread_exit(NULL);
}

int sorter2(char *lines[], int totalLines, char *sortColumn){

    char  input[MSG_SIZE] = {0};

    int foundSortCol=0;
    char **cols;
    int sortColIdx=0;

    int arraySize=1000;
    //28 total columns with a null entry at end

    // double pointer to store all rows, will be used in merge sort
    rowType ** rows=malloc(arraySize * sizeof(rowType *));
    // printf("initial size of colTypes:%d\n", sizeof(colTypes)/sizeof(colTypes[0]));

    //error check in case input has an invalid column
    if ((getColType(colTypes, sortColumn))==NULL){
        fprintf(stderr,"%s is not a valid column\n", sortColumn);
        exit(1);
    }
    // process header line
    int length = strlen(lines[0]);
    memcpy(input, lines[0], length);

    // split function for initial header columns

    doTrim(input, length);
    cols=split(input, ',');
    for(int i=0;i<28;i++){
        if(strcmp(cols[i], sortColumn)==0){
            foundSortCol=1;
            // stores the index of designated column to sort on
            sortColIdx=i;
            break;
        }
    }
    if(foundSortCol==0){
        fprintf(stderr,"unable to find column header line %s", sortColumn);
        return -1;
    }


    int lineCount = 0;
    for (int idx = 1; idx < totalLines; idx++) {

        length = strlen(lines[idx]);

        memset(input, '\0', MSG_SIZE);

        memcpy(input, lines[idx], length);

        // printf("read: %d bytes\n", bytesRead);
        // takes off unneeded chars at end of line, and parsing for the rest of input
        doTrim(input,length);
        cols=split(input, ',');

        int colIdx=0;
        if(lineCount == arraySize){
            arraySize*=2;
            rows=realloc(rows, arraySize*sizeof(rowType *));
            //   printf("more memory allocated");
        }
        rows[lineCount]=malloc(29*sizeof(colEntry));
        // get data from line
        for(colIdx=0;colIdx<28;colIdx++){
            char * colName=colTypes[colIdx].name;
            char * value=NULL;
            if(strlen(cols[colIdx])==0){
                value="";
            }
            else{
                int len=strlen(cols[colIdx]);
                value=malloc(len+1);
                memset(value, '\0', len+1);
                memcpy(value, cols[colIdx], len);
            }
            colEntry thisCol={colName, value};
            // line data stored in rows
            memcpy(&rows[lineCount]->colEntries[colIdx], &thisCol, sizeof(colEntry));

        }
        // error checking for invalid line entries
        if(colIdx<27){
            printf("only %d columns found in line %d %s", colIdx, lineCount, input);
            exit(1);
        }

        lineCount++;
    }


    doSort(rows,sortColIdx,colTypes[sortColIdx].type,lineCount);

    pthread_mutex_lock(&rowLock);

    /*
     * Add the sorted output of this file to the global allRows structure
     */
    int startRow = allRows.rowCount;
    allRows.rowCount += lineCount;
    if (allRows.rows == NULL) {
        allRows.rows = malloc(allRowsArraySize * sizeof(rowType *));
    }
    else if (allRows.rowCount >= allRowsArraySize){
        allRowsArraySize *= 2;
        allRows.rows = realloc(allRows.rows, allRowsArraySize*sizeof(rowType *));
    }

    for (int i = 0; i < lineCount; i++, startRow++) {
        allRows.rows[startRow] = rows[i];

    }

    pthread_mutex_unlock(&rowLock);

    return 0;

}
char** split(char* line, char delimiter){

    char * p1,* p2,* p3=NULL;
    char ** cols=calloc(1024,sizeof(char *));
    // strchr returns pointer after a ',' is found
    p2=strchr(line, delimiter);
    // p1 receives original pointer
    p1=line;
    int colCount=0;
    long len=0;
    while(p2!=NULL){
        // p2 has a larger address, byte difference plus one assigned to len
        len=(p2-p1)+1;
        // space allocated for column entry, memory assigned
        p3= malloc(len*sizeof(char *));
        memset(p3, '\0', len);
        memcpy(p3, p1, len-1);
        // specified location assigned for value
        cols[colCount]=p3;
        // pointers moved
        p1=p2+1;
        if(*p1=='"'){
            p2=strchr(p1+1,'"');
            if(p2!=NULL){
                p2=strchr(p1,delimiter);
            }
        }
        else{
            p2=strchr(p1, delimiter);
        }

        colCount++;
    }
    // for getting the last value in a line
    if(p1 !=NULL&&strlen(p1)>0){
        len=strlen(p1)+1;
        p3=malloc(len*sizeof(char *));
        memset(p3, '\0', len);
        memcpy(p3, p1, len-1);
        cols[colCount]=p3;
    }
    return cols;
}


int isCommand(char *input) {

    if (strcmp(input, SOD) == 0
        || strcmp(input, EOD) == 0
        || strcmp(input, SOC) == 0
        || strcmp(input, EOC) == 0
        || strcmp(input, DMP) == 0 )
        return 1;
    else return 0;
}

void setColumns(column colTypes[]){
    colTypes[0]=(column){"color", "char"};
    colTypes[1]=(column){"director_name","char"};
    colTypes[2]=(column){"num_critic_for_reviews","long"};
    colTypes[3]=(column){"duration","long"};
    colTypes[4]=(column){"director_facebook_likes","long"};
    colTypes[5]=(column){"actor_3_facebook_likes","long"};
    colTypes[6]=(column){"actor_2_name","char"};
    colTypes[7]=(column){"actor_1_facebook_likes","long"};
    colTypes[8]=(column){"gross","long"};
    colTypes[9]=(column){"genres","char"};
    colTypes[10]=(column){"actor_1_name","char"};
    colTypes[11]=(column){"movie_title","char"};
    colTypes[12]=(column){"num_voted_users","long"};
    colTypes[13]=(column){"cast_total_facebook_likes","long"};
    colTypes[14]=(column){"actor_3_name","char"};
    colTypes[15]=(column){"facenumber_in_poster","long"};
    colTypes[16]=(column){"plot_keywords","char"};
    colTypes[17]=(column){"movie_imdb_link","char"};
    colTypes[18]=(column){"num_user_for_reviews","char"};
    colTypes[19]=(column){"language","char"};
    colTypes[20]=(column){"country","char"};
    colTypes[21]=(column){"content_rating","char"};
    colTypes[22]=(column){"budget","long"};
    colTypes[23]=(column){"title_year","long"};
    colTypes[24]=(column){"actor_2_facebook_likes","long"};
    colTypes[25]=(column){"imdb_score","float"};
    colTypes[26]=(column){"aspect_ratio","float"};
    colTypes[27]=(column){"movie_facebook_likes","long"};
    colTypes[28]=(column){NULL  ,NULL};
}

int getColIndex(char *column) {
    int i = 0;
    while(colTypes[i].name != NULL) {
        if (strcmp(colTypes[i].name, column) == 0) {
            return i;
        }
        i++;
    }
    return -1;
}

char * getColType(column colTypes[], const char * sortcolumn){
    int i=0;

    while(colTypes[i].name!=NULL){
        // column thisCol=colTypes[i];
        if(strcmp(colTypes[i].name, sortcolumn)==0){
            return colTypes[i].type;
        }
        i++;
    }
    return NULL;
}


void doTrim(char input[], size_t size) {
    if (input[size-1]=='\n'){
        input[size-1]='\0';
    }
    if (input[size-2]=='\r'){
        input[size-2]='\0';
    }
}

int doSend(int sockFd, char *msg) {

    char buffer[2048] = {0};

    int len = strlen(msg);

    memcpy(buffer, &len, sizeof(int));
    memcpy(buffer + sizeof(int), msg, len);

    return send(sockFd, buffer, len+ sizeof(int), 0);

}

int doRead(int sockFd, char *buffer) {

    int len;
    int bytesRead = read(sockFd, &len, sizeof(int));

//    printf("read %d bytes msg length %d\n", bytesRead, len);

    bytesRead = read(sockFd, buffer, len);

//    printf("read %d bytes msg %s\n", bytesRead, buffer);

    return bytesRead;
}

void printRows(rowType **rows, int rowCount, int sockFd) {

    char outBuffer[2048] = {0};

    for (int i = 0; i < rowCount; i++) {

        rowType *row = rows[i];

        if (row == NULL) {
            fprintf(stderr, "Found null row at line: %d\n", i);
            return;
        }

        for (int j = 0; j < 28; j++) {

            char *vp = row->colEntries[j].value;
            if (j > 0)
                strcat(outBuffer, ",");
            strcat(outBuffer, (vp == NULL) ? "" : vp);
        }

        doSend(sockFd, outBuffer);
        memset(outBuffer, '\0', sizeof(outBuffer));
    }
}