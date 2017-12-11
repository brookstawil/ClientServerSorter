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
/*
Don't need the sort_file thread arguments anymore
typedef struct threadArg{
    int* sd;
	char* pathName;
	char* directoryName;
	FILE* csvFile;
	char* directory_path;
	char* output_dir;
	int counter;
} args_sortFile;
*/

//arguments for sending file data across some socket file descriptor
typedef struct threadArg{
    int* fdptr;
	FILE* csvFile;
} args_sendFileData;

//These arguments are used by the directoryTraversing threads
typedef struct threadArg2{
	DIR* directory;
	char* directory_path;
	int counter;
	pthread_t* threadHolder;
	char* output_dir;
} args_travelDirectory;

//arguments for stackPoping
//Each thread allocates memory for these arguments, and are used when poping sorted rows from the stack
//These arguments are used by the fileSorting threads
typedef struct threadArg3{
    Row ** row1;
    Row ** row2;
} args_sortedRowStackPop;

//arguments for a thread that watches the connection on some socket an d waits for termination
typedef struct threadArg4{
    int* fdptr;
} args_watchConnection;

//arguments for a thread that watches the connection on some socket and recives sorted files
typedef struct threadArg5{
    int* fdptr;
    FILE* csvFileOut;
} args_receiveFileData;

int travdir(const char * input_dir_path, char* column_to_sort, const char * output_dir);
void createSocket(void* margs);
void goThroughPath(void* margs2);
args_sortFile * createThreadsTransmit(char* pathname, char* d_name, FILE* csvFile, char* output_dir, char* directory_path, int counter);
args_travelDirectory * createThreadsTraverse(char * output_dir, int counter, pthread_t* threadHolder, DIR * directory, char *directory_path);
args_watchConnection * createThreadsWatchConnection(int *fdptr);
args_receiveFileData * createThreadsReceiveFileData(int *fdptr);
int isAlreadySorted(char *pathname,char *column_to_sort);
void printToCSV(FILE *csv_out, Row ** rows, int numRows, int numCols);
int parseData(char *lines[], int totalLines);
