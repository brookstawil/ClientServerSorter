#include <stdio.h>
#include <sys/socket.h>
#include <stdlib.h>
#include <netinet/in.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <pthread.h>
#include <dirent.h>
#include <errno.h>
#include <semaphore.h>

#define  SOD "SOD"
#define  EOD "EOD"
#define SOC "SOC"
#define EOC "EOC"
#define DMP "DMP"

int infoCheck(char path[]);
void doTrim(char input[], size_t size);
void * listdir(void *name);
int doSend(int sockFd, char *msg);
int doRead(int sockFd, char *buffer);
void * sendFile(void *path);
char** split(char* line, char delimiter);
int getSocket(char *host, char  *port);

char *sortColumn;
char *host;
char *port;
char *clientPoolSize;

pthread_mutex_t countLock;
pthread_mutex_t socketLock;
sem_t client_pool;
const int MAX_THREADS = 4096;

static int threadCount;

int main(int argc, const char * argv[]) {

    char cwd[1024];

    const char* usageMsg ="Usage: sorter_client -c <column name> -h <host> -p <port> [-d <source directory>] [-o <output directory>]";

    if (argc < 3) {
        fprintf(stderr, "%s\n", usageMsg);
        exit(1);
    }
    int c;

    const char *sourceDir = NULL;
    const char *outDir = NULL;

    while ((c = getopt (argc, argv, "c:d:o:h:p:s")) != -1)
        switch (c)
        {

            case 'c':
                sortColumn = optarg;
                break;
            case 'd':
                sourceDir = optarg;
                break;
            case 'h':
                host = optarg;
                break;

            case 'o':
                outDir = optarg;
                break;
            case 'p':
                port = optarg;
                break;
            case 's':
                clientPoolSize = optarg;
                break;

            case '?':
                fprintf (stderr,
                         "Unknown option character `\\x%x'.\n",
                         optopt);
                return 1;
            default:
                abort ();
        }

    if (sortColumn == NULL || host == NULL || port == NULL) {
        fprintf(stderr,"%s\n", usageMsg);
        exit(1);
    }

    if (sourceDir == NULL) {

        getcwd(cwd, sizeof(cwd));
        sourceDir = cwd;
    }

    if (clientPoolSize == NULL) {
        clientPoolSize = "10";
    }


    if (pthread_mutex_init(&countLock, NULL) != 0)
    {
        int errnum = errno;
        fprintf(stderr, "Failed to initialize countLock mutex: %s\n",  strerror(errnum));

        exit(1);
    }

    if (pthread_mutex_init(&socketLock, NULL) != 0)
    {
        int errnum = errno;
        fprintf(stderr, "Failed to initialize rowLock mutex: %s\n",  strerror(errnum));

        exit(1);
    }

    fflush(stdout);

    pthread_t tid = 0;
    sem_init(&client_pool, 0, atoi(clientPoolSize));

    pthread_create(&tid, NULL, listdir, (void *) sourceDir);

    printf("%ld,", tid);
    pthread_join(tid, NULL);
//    listdir(sourceDir);

    threadCount++;  // add one for the initial thread
    printf("\n\tTotal number of threads: %d\n", threadCount);
    //    printf("main is exiting\n");

    pthread_mutex_destroy(&countLock);
    pthread_mutex_destroy(&socketLock);

    char *fileName = calloc(1024, sizeof(char *));
    strcat(fileName, "AllFiles-sorted-");
    strcat(fileName, sortColumn);
    strcat(fileName, ".csv");

    char *outFilename;
    long len = 0;

    if (outDir == NULL) {
        outDir = sourceDir;
    }

    len = strlen(outDir) + strlen(fileName) + 30;
    outFilename=calloc(len, sizeof(char *));
    strncpy(outFilename, outDir, strlen(outDir));
    strcat(outFilename, "/");
    strncat(outFilename, fileName, fileName);
    //        printf("outFilename: %s\n", outFilename);

    FILE* ofp = fopen(outFilename, "w");

    if (ofp == NULL) {
        int errnum = errno;
        fprintf(stderr, "Failed to open %s (%s)\n", outFilename, strerror(errnum));
        exit(1);
    }
    printf("writing to file: %s\n", outFilename);

    sem_wait(&client_pool);

    int sockfd = getSocket(host, port);

    sem_post(&client_pool);

    printf("Sending 'DMP' command\n");
    doSend(sockfd, DMP);

    char buffer[2048] = {0};

    int moreData = 1;

    while(moreData) {
        int bytesRead = doRead(sockfd, buffer);

        if (strcmp(buffer, EOD) == 0) {
            moreData = 0;
        }
        else {
            fprintf(ofp, "%s\n", buffer);
        }
        memset(buffer, '\0', sizeof(buffer));

    }

    fclose(ofp);
    sem_destroy(&client_pool);
    exit(0);
}


void * listdir(void * name) {
    DIR *dir = NULL;
    int i=0;

    struct dirent *entry = NULL;
    pthread_t tid[MAX_THREADS];
//    char paths[2048][1024] = {{0}};
    char ** paths=calloc(MAX_THREADS,sizeof(char *));

    int count = 0;
    if (!(dir = opendir(name)))
        pthread_exit(name);

//    printf("In listdir current tid is: %i\n", pthread_self());
    while ((entry = readdir(dir)) != NULL) {
        if (entry->d_type == DT_DIR) {
            char *path  = calloc(1024, sizeof(char*));
            //memset(path, '\0', 1024);
            if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
                continue;
            // sprintf(path, "%s/%s", thisPath, entry->d_name);
            //printf("%*s[%s]\n", indent, "", entry->d_name);

            sprintf(path, "%s/%s", name, entry->d_name);
            paths[count] = path;
            //sprintf(&paths[count], path, strlen(path));
            pthread_create(&tid[count], NULL, listdir, paths[count]);
//            pthread_join(tid[count], NULL);
//            printf("listdir thread: %i has completed for dir %s\n", tid, path);
            pthread_mutex_lock(&countLock);
            threadCount = threadCount + 1;
            count++;

            pthread_mutex_unlock(&countLock);


        } else if (strrchr(entry->d_name, '.') != NULL) {
            if (strcmp(strrchr(entry->d_name, '.'), ".csv") == 0) {

                char *path  = calloc(2048, sizeof(char*));
                //memset(path, '\0', 1024);
                sprintf(path, "%s/%s", name, entry->d_name);

                if (infoCheck(path) == 0) {
                    continue;
                } else {
                    //memcpy(&paths[count], path, strlen(path));
                    //	sprintf(paths[count], "%s/%s", name, entry->d_name);
                    paths[count] = path;
                    if (pthread_create(&tid[count], NULL, sendFile, paths[count]) != 0) {
                        perror("failed to created sorter2 thread");
                        pthread_exit(NULL);
                    }

                    pthread_mutex_lock(&countLock);
                    threadCount = threadCount + 1;
                    count++;

                    pthread_mutex_unlock(&countLock);

                }
            }
        }

    }

    closedir(dir);

    for (i =0; i < count; i++) {
        pthread_join(tid[i], NULL);
//        printf("thread: %ld has completed \n", tid[i]);
    }

//    printf("this thread id is %d\n", pthread_self());

    pthread_exit(name);

}

void * sendFile(void *path) {

    printf("sending file: %s\n", path);
    int bytesRead;

    char buffer[2048] = {0};
    size_t buffSize=1024;
    FILE * fp=fopen(path, "r");

    sem_wait(&client_pool);

    int sockfd = getSocket(host, port);

    sem_post(&client_pool);

    if (sockfd == -1) {
        perror("Failed to get socket");
        return NULL;
    }

    if (fp == NULL) {

        perror("Failed to open file");
        close(sockfd);
        return NULL;
    }


    char *input=(char *) malloc(buffSize+1);
    while((bytesRead=getline(&input, &buffSize, fp))!=-1){
        doTrim(input, bytesRead);
        doSend(sockfd, input);
    }

    doSend(sockfd, EOD);
    fclose(fp);

    doSend(sockfd, sortColumn);

    doSend(sockfd, EOC);

    //Wait for server to complete the sort
    bytesRead = doRead(sockfd, buffer);
    printf("%s\n", buffer);

    close(sockfd);
    free(input);
    return 0;
}

int doSend(int sockFd, char *msg) {

    char buffer[2048] = {0};

    int msgLen = strlen(msg);
    size_t netLen = htonl(msgLen);

    memcpy(buffer, &netLen, sizeof(size_t));
    memcpy(buffer + sizeof(size_t), msg, msgLen);

    return send(sockFd, (void *)buffer, msgLen+ sizeof(size_t), 0);

}

int doRead(int sockFd, char *buffer) {

    size_t len;
//    char lenBuff[_SIZE_T] = {0};
    ssize_t totalBytesRead = 0;
    while (totalBytesRead < sizeof(len)) {
        ssize_t bytesRead = read(sockFd, &len + totalBytesRead, sizeof(size_t) - totalBytesRead);
        totalBytesRead += bytesRead;
    }

    len = ntohl(len);

//    printf("read %d bytes msg length %d\n", bytesRead, len);

    totalBytesRead = 0;
    while(totalBytesRead < len) {
        ssize_t  bytesRead = read(sockFd, buffer + totalBytesRead, len - totalBytesRead);
        totalBytesRead += bytesRead;
    }


//    printf("read %d bytes msg %s\n", bytesRead, buffer);

    return totalBytesRead;
}

int getSocket(char *host, char  *port) {

    struct sockaddr_in serv_addr;
    int sockfd;
    struct addrinfo hints, *servinfo, *p;
    int rv;

    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;

    if ((rv = getaddrinfo(host, port, &hints, &servinfo)) != 0) {
        fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(rv));
        return(-1);
    }

    memset(&serv_addr, '0', sizeof(serv_addr));

    pthread_mutex_lock(&countLock);
    for(p = servinfo; p != NULL; p = p->ai_next) {
        if ((sockfd = socket(p->ai_family, p->ai_socktype,
                             p->ai_protocol)) == -1) {
            perror("socket");
            continue;
        }

        if (connect(sockfd, p->ai_addr, p->ai_addrlen) == -1) {
            perror("connect");
            close(sockfd);
            continue;
        }

        break; // if we get here, we must have connected successfully
    }
    pthread_mutex_unlock(&countLock);
    if (p == NULL) {
        // looped off the end of the list with no connection
        fprintf(stderr, "failed to connect\n");
        return -1;
    }
    return sockfd;
}

void doTrim(char input[], size_t size) {
    if (input[size-1]=='\n'){
        input[size-1]='\0';
    }
    if (input[size-2]=='\r'){
        input[size-2]='\0';
    }
}

int infoCheck(char path[]){
    FILE * fp=fopen(path, "r");

    if (fp == NULL) {
        fprintf(stderr, "infoCheck: failed to open file: %s (%s)\n", path, strerror(errno));
        exit(1);
    }
    size_t buffSize=1024;
    char **cols;
    char * input;
    input=(char *) malloc(buffSize+1);
    int bytesRead=getline(&input, &buffSize, fp);

    fclose(fp);
    if(bytesRead==-1){
        return 0;
    }
    doTrim(input, bytesRead);
    cols=split(input, ',');
    int i=0;
    while(cols[i]!=NULL){
        i++;
    }
    if(i==28){
        return 1;
    }
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