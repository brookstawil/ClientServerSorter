#include <unistd.h>
#include <pthread.h>

struct stat {
    dev_t     st_dev;     /* ID of device containing file */
    ino_t     st_ino;     /* inode number */
    mode_t    st_mode;    /* protection */
    nlink_t   st_nlink;   /* number of hard links */
    uid_t     st_uid;     /* user ID of owner */
    gid_t     st_gid;     /* group ID of owner */
    dev_t     st_rdev;    /* device ID (if special file) */
    off_t     st_size;    /* total size, in bytes */
    blksize_t st_blksize; /* blocksize for file system I/O */
    blkcnt_t  st_blocks;  /* number of 512B blocks allocated */
    time_t    st_atime;   /* time of last access */
    time_t    st_mtime;   /* time of last modification */
    time_t    st_ctime;   /* time of last status change */
};

//arguments for processfiletosort
//Each thread allocates memory for these arguments, and are used throughout the sorting process.
//These arguments are used by the fileSorting threads

//arguments for sending file data across some socket file descriptor
typedef struct threadArg{
    int* fdptr;
	int csvFile;
} args_sendFileData;

//These arguments are used by the directoryTraversing threads
typedef struct threadArg2{
	DIR* directory;
	char* directory_path;
	int counter;
	pthread_t* threadHolder;
	char* output_dir;
} args_travelDirectory;

//arguments for a thread that watches the connection on some socket and recives sorted files
typedef struct threadArg3{
    int* fdptr;
    int csvFileOut;
} args_receiveAndWriteFileData;

int travdir(const char * input_dir_path, char* column_to_sort, const char * output_dir);
int createSocket(const char * server, const char * port);
void goThroughPath(void* margs2);
args_travelDirectory * createThreadsTraverse(char * output_dir, pthread_t* threadHolder, DIR * directory, char *directory_path);
args_receiveAndWriteFileData * createThreadsReceiveAndWriteFileData(int *fdptr, FILE* csvFileOut);
args_sendFileData * createThreadsSendFileData(int *fdptr, int csvFile);
int isAlreadySorted(char *pathname,char *column_to_sort);
int parseData(char *lines[], int totalLines);
int doSend(int sockFd, char *msg);
int doRead(int sockFd, char *buffer);
void doTrim(char input[], size_t size);
