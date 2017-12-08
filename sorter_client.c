#include <stdio.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <unistd.h>
#include <dirent.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <time.h>
#include <pthread.h>
#include <sys/poll.h>
#include "sorterthreading.c"
#include "sorter_client.h"
#include "stack.c"

#define MAX_PATH_LENGTH 256

//the root is now an initial thread process
pid_t root;
pthread_t rootTID;
stack_safe *StackOfSortedFiles;
int numRows2;
int counterofthreads;
pthread_t* threadIds;
char *global_output_dir;
char *global_starting_dir;
char *global_column_to_sort;
char *global_serverAddress;
char *global_portNum;

int main (int argc, char* argv[]) {

	//We want to ensure that only the root thread does certain operations
	root = getpid();
	rootTID = pthread_self();

	char *errorMessage = "\n";
	int i; 
	for (i = 0; i < argc; i++) { 
		//printf("%s\n", argv[i]); 
		char* argument = argv[i];
		if(strcmp(argument,"-d") == 0){
			global_starting_dir = argv[i+1];
		} else if(strcmp(argument,"-c") == 0) {
			global_column_to_sort = argv[i+1];
		} else if(strcmp(argument,"-o") == 0){
			global_output_dir = argv[i+1];
		} else if(strcmp(argument, "-p")==0){
			global_portNum = argv[i+1];
		} else if(strcmp(argument, "-h")==0){
			global_serverAddress = argv[i+1];
		}
	}

	/** We now have the IP address and port to connect to on the server, we have to get    **/
	/**   that information into C's special address struct for connecting sockets    **/

	//Required arguments check
	if(global_column_to_sort == NULL) {
		printf("ERROR: No input column specified. Column to sort must be given with argument tag '-c'.\n");
		exit(0);
	}

	if(global_serverAddress == NULL) {
		printf("ERROR: No server address specified. Server address must be given with argument tag '-h'.\n");
		exit(0);
	}

	if(global_portNum == NULL) {
		printf("ERROR: No port number specified. Port number must be given with argument tag '-h'.\n");
		exit(0);
	}

	//Optional arguments default behavior
	if(global_starting_dir == NULL) {
		printf("Staring directory not specified, defaulting to './' as the global_starting_dir.\n");
		global_starting_dir = ".";
	}

	if(global_output_dir == NULL) {
		printf("Output directory not specified, defaulting to './' as the global_output_dir.\n");
		global_output_dir = "."; //Will either be a valid global_starting_dir or ./ at this point in code
	}

	StackOfSortedFiles = stack_create(30);

	travdir(global_starting_dir, global_column_to_sort, global_output_dir);	

	return 0;
}

//A watchConnection thread will also be given the socket file descriptor (and other information?) passed in an arguments struct
//It will wait for responses on the socket.
void* watchConnection(void* args) 
{
	args_watchConnection* watchConnectionArgs = args;

	int sd;
	int i;
	char buffer[512];
	char output[512];

	sd = *(watchConnectionArgs->fdptr);
	while( (i = recv( sd, buffer, sizeof(buffer),0) ) != 0 )
	{
		sprintf( output, ">%s<\n", buffer );
		write( 1, output, strlen(output) );

		if(strcmp(buffer,"SOME DISCONNECT SIGNAL")==0)
		{
			exit(1);
		}
		sleep(1);
	}

	sprintf( output, "Server disconnected\n" );
	write( 1, output, strlen(output) );
	close(sd);
	exit(1);
}

//A sendFileData thread will have the socket file descriptor (and other information?) passed in an arguments struct
void* sendFileData(void* args) 
{
	/*
		We must transmit:
			1. The size of the line
			2. The line itself
			3. Some ending signal once the file is done
	*/
}

int createSocket(const char * server, const char * port)
{
	int	sd;
	struct addrinfo	addrinfo;
	struct addrinfo *result;
	char message[256];

	addrinfo.ai_flags = 0;
	addrinfo.ai_family = AF_INET;		// IPv4 only
	addrinfo.ai_socktype = SOCK_STREAM;	// Want TCP/IP
	addrinfo.ai_protocol = 0;		// Any protocol
	addrinfo.ai_addrlen = 0;
	addrinfo.ai_addr = NULL;
	addrinfo.ai_canonname = NULL;
	addrinfo.ai_next = NULL;
	if ( getaddrinfo(server, port, &addrinfo, &result ) != 0 )
	{
		fprintf( stderr, "\x1b[1;31mgetaddrinfo( %s ) failed.  File %s line %d.\x1b[0m\n", server, __FILE__, __LINE__ );
		return -1;
	}
	else if ( errno = 0, (sd = socket( result->ai_family, result->ai_socktype, result->ai_protocol )) == -1 )
	{
		freeaddrinfo( result );
		return -1;
	}
	else
	{
		do {
			if ( errno = 0, connect( sd, result->ai_addr, result->ai_addrlen ) == -1 )
			{
				sleep( 3 );
				write( 1, message, sprintf( message, "\x1b[2;33mConnecting to server %s ...\x1b[0m\n", server ) );
			}
			else
			{
				freeaddrinfo( result );
				return sd;		// connect() succeeded
			}
		} while ( errno == ECONNREFUSED );
		freeaddrinfo( result );
		return -1;
	}
}

//open the directory and create threadholder
int travdir(const char * input_dir_path, char* column_to_sort, const char * output_dir)
{
	numRows2 = 0;
	char *directory_path = (char *) malloc(MAX_PATH_LENGTH);
	strcpy(directory_path, input_dir_path);
	
	DIR * directory = opendir(directory_path);

	if (directory == NULL) {
        return 1;
	}

	//have one thread go through directories
	int numThreads = 50;
	pthread_t* threadHolder = (pthread_t*)(malloc(sizeof(pthread_t) * numThreads));
	goThroughPath(createThreadsTraverse(output_dir, counterofthreads, threadHolder, directory, directory_path));

	free(directory_path);
	return 0;
}
	
//Function pointer to go through the directory path and finds csvs.
//Arguments are set without a helper are are set in this function
void goThroughPath(void* args)
{
	args_travelDirectory* travelDirectoryArgs = args;
	DIR* directory = travelDirectoryArgs->directory;
	char* directory_path = travelDirectoryArgs->directory_path;
	pthread_t* threadHolder = travelDirectoryArgs->threadHolder;
	char* finalDirectoryPath;
	char* output_dir = global_output_dir;
	counterofthreads++;

	//while we go through the directory -> in the parent process keep looking for csv files
	while(directory != NULL) 
	{
		struct dirent * currEntry;
		char * d_name;
		currEntry = readdir(directory);

		//making sure not to over thread bc memory
		if(travelDirectoryArgs->counter == 256){
			break;
		}
		//end of file stream, break-> now wait for children and children's children
		if(!currEntry) {
			break;
		}
		//d_name is the current directory/file
		d_name = currEntry->d_name;

		//this is a directory 
		if(currEntry->d_type==DT_DIR) 
		{
			if(strcmp(d_name,".") != 0 && strcmp(d_name, "..") != 0) {
				//need to EXTEND THE PATH for next travdir call, working dir doesn't change (think adir/ -> adir/bdir/....)
				int pathlength = 0;	
				char path[MAX_PATH_LENGTH];
				
				pathlength = snprintf(path, MAX_PATH_LENGTH, "%s/%s",currEntry, d_name);
				if(pathlength > MAX_PATH_LENGTH-1) {
					printf("ERROR: Path length is too long");
					return;
				}

				char * newDirectoryPath = (char *)malloc(strlen(directory_path) + strlen(d_name) + 2);
				strcpy(newDirectoryPath, directory_path);

				//open new directory again
				strcat(newDirectoryPath,"/");
				strcat(newDirectoryPath,d_name);
				DIR * newDirectory = opendir(newDirectoryPath);

				if(*global_output_dir != 0){
					if(*d_name != 0){
						if(strcmp(d_name,global_output_dir)==0){
							finalDirectoryPath = (char *)calloc(1, strlen(directory_path) + strlen(global_output_dir) + sizeof("/AllFiles-sorted-") + sizeof(global_column_to_sort) + sizeof(".csv") + 3);
							strcpy(finalDirectoryPath,newDirectoryPath);
						}
					}
				}

				//We have found a new directory and must thus make a new thread for it.
				//This requires updating the counter of the parent directory, as well as adding the thread to the threadholder
				pthread_t thread;
				pthread_create(&thread, 0, goThroughPath, createThreadsTraverse(output_dir, 0, threadHolder, newDirectory, newDirectoryPath));
				travelDirectoryArgs->threadHolder[travelDirectoryArgs->counter++] = thread;
			}
		} 
		else if(currEntry->d_type == DT_REG) 
		{ 	//regular files, need to check to ensure ".csv"

			char pathname [256];
			FILE* csvFile;
			sprintf(pathname, "%s/%s", directory_path, d_name);

			//Check to see if the file is a csv
			char *lastdot = strrchr(d_name, '.');

			if (lastdot == NULL || strcmp(lastdot,".csv") != 0) {
				printf("File is not a .csv: %s\n", d_name);
			} else if(isAlreadySorted(pathname, global_column_to_sort)) {
				printf("File already sorted: %s\n", d_name);
				break;
			} else {
				csvFile = fopen(pathname, "r");
				if (csvFile != NULL) {
					//pathname has the full path to the file including extension
					//directory_path has only the parent directories of the file

					/*
						This is the location where a server connection is made. 
						The call to createSocket(serverAddress, portNum) will return some file descriptor. 
						This helper function essentially holds all of the network programming needed to get a socket going.
						We must obviously check to see if the socket was valid by checking the return value
					*/

					/*
						We must transmit:
							1. The size of the line
							2. The line itself
							3. Some ending signal once the file is done
						This is to be implemented in the sendFileData() thread function described above.
					*/

					int* sd;
					if ( (sd = createSocket( serverAddress, portNum )) == -1 ) {
						write( 1, message, sprintf( message,  "\x1b[1;31mCould not connect to server %s errno %s\x1b[0m\n", argv[1], strerror( errno ) ) );
						return 1;
					} else {
						//With a valid file descriptor we can then split off into another thread which will deal with parsing the file and writing to the socket.
						printf( "Connected to server %s\n", serverAddress);
						fdptr = (int *)malloc(sizeof(int));
						*fdptr = sd;

						//Once we have the socket file pointer we can create a new thread, call it sendFileData
						//TODO: create some arg structure for this thread, we need at least the fdptr, what else do we need?
						pthread_t sendFileData_thread, watchConnection_thread;
						pthread_create(&sendFileData_thread, NULL, sendFileData, createThreadsSendFileData(fdptr, ));
						//We must also spawn a watchConnection thread which will wait for the server to respond
						pthread_create(&watchConnection_thread, NULL, watchConnection, createThreadsWatchConnection(fdptr, ))

						/*
							In this implementation the client will join connection threads just after creation. 
							Meaning that traversing to the next file in the directory will not happen until the file has been sent.
							Do we need to join or can we just let these threads run?
							This is going based off the decription that Tjang gave in the assignment page.
						*/

						pthread_join(sendFileData_thread, NULL);
						pthread_join(watchConnection_thread, NULL);

						
						free(fdptr);
						close(sd);
						return 1; //Thread on a file returns 1 when finished
					}
				}
			}	
		} else {
			printf("ERROR: Not a valid file or directory\n");
		}	
	}

	/*
		At this point all of the files have their data being sent. 
		We first must join all of the directory threads before we make a final request for the sorted files.
	*/

	//Join the directory threads  to finish 
	int i=0,j=0;
	for(i = 0; i < travelDirectoryArgs->counter; i++){
		pthread_join(travelDirectoryArgs->threadHolder[i], NULL);
	}

	free(travelDirectoryArgs->directory);


	/*	
		At this point in execution any given directory thread should have finished with its children.
		Only the root thread should be requesting for the sorted files, otherwise let the individual directory threads exit.
	*/


	//Anything that occurs in this conditional will only be done by the root thread
	if(getpid() == root && pthread_self() == rootTID)
	{
		int rowSet1Length=0;
		int rowSet2Length=0;
		int totalthreads = travelDirectoryArgs->counter;

		printf("TIDS of all child threads: ");
		for(i = 0; i < counterofthreads; i++){
			printf("%u,", threadIds[i]);
		}

		free(threadIds);
		printf("\n");
		printf("Total number of threads: %d\n\r", counterofthreads);
		
		//In this implementation we will pop and push from the stack once the threads have all completed
		//While the stack is not empty, we pop twice and merge.
		//If one of the Row ** that we pop is empty then we must have all of the rows merged together and can exit
		printf("\n");

		Row ** sortedRows;

		while(!is_empty(StackOfSortedFiles)) {
			//printf("The stack has %d elements in it.\n", StackOfSortedFiles->count);
			if(StackOfSortedFiles->count > 1) {

				Row ** rowSet1 = pop(StackOfSortedFiles);
				Row ** rowSet2 = pop(StackOfSortedFiles);
				rowSet1Length = getAmountOfRows(rowSet1);
				rowSet2Length = getAmountOfRows(rowSet2);
				sortedRows = malloc(sizeof(Row) * (rowSet1Length + rowSet2Length + 1));

				if(sortedRows != NULL) {

					mergeRowsTwoFinger(sortedRows, rowSet1, rowSet2);
					push(StackOfSortedFiles, sortedRows);

				} else {

				}
			} else if (StackOfSortedFiles->count == 1) {

				sortedRows = pop(StackOfSortedFiles);
				rowSet1Length = getAmountOfRows(sortedRows);

			}
		}
		printf("The stack is now empty. The rows are now stored in sortedRows.\n");

		printf("The rows will now be printed to the masterCSV file.\n");

		//open the output directory again and put the csv file there.
		if(*global_output_dir=='.'){
			finalDirectoryPath = (char *)calloc(1, strlen(global_output_dir) + sizeof("/AllFiles-sorted-") + sizeof(global_column_to_sort) + sizeof(".csv") + 5);
			strcat(finalDirectoryPath, global_output_dir);
		}

		if(finalDirectoryPath[strlen(finalDirectoryPath) - 1] == '/') {
			strcat(finalDirectoryPath,"AllFiles-sorted-");
		} else {
			strcat(finalDirectoryPath,"/AllFiles-sorted-");
		}

		strcat(finalDirectoryPath, global_column_to_sort);
		strcat(finalDirectoryPath,".csv");

		FILE *csvFileOut = fopen(finalDirectoryPath,"w");

		printToCSV(csvFileOut, sortedRows, rowSet1Length, NUM_COLS);

		stack_destroy(StackOfSortedFiles);

		//free the rows poped from the stack
		for(i = 0; i < rowSet1Length; i++) {
			//for(j = 0; j < NUM_COLS; j++) {
				//free(sortedRows[i]->colEntries[j].value);
				//sortedRows[i]->colEntries[j].value = NULL;
				//free(sortedRows[i]->colEntries[j].type);
				//sortedRows[i]->colEntries[j].type = NULL;
			//}
			free(sortedRows[i]);
			sortedRows[i] = NULL;
		}

		free(sortedRows);
		sortedRows = NULL;
		
		free(finalDirectoryPath);
		free(args);
		exit(0);
	}

	pthread_exit(NULL);
}

//Helper function that sets the arguments for a thread that transmits a given file 
args_sendFileData * createThreadsSendFileData(int *fdptr) {
	args_sendFileData* sendFileDataArgs = (args_sendFileData *)malloc(sizeof(args_sendFileData));
	args_sendFileData->fdptr = fdptr;
	args_sendFileData->pathName = pathName;
	args_sendFileData->directoryName = directoryName;
	args_sendFileData->csvFile = csvFile;
	args_sendFileData->directory_path = directory_path;
	args_sendFileData->counter = counter;

	return sendFileDataArgs;
}

args_travelDirectory * createThreadsTraverse(char * output_dir, int counter, pthread_t* threadHolder, DIR * directory, char *directory_path){
	args_travelDirectory* travelDirectoryArgs = (args_travelDirectory *)malloc(sizeof(args_travelDirectory));
	travelDirectoryArgs->output_dir = output_dir;
	travelDirectoryArgs->counter = counter;
	travelDirectoryArgs->threadHolder = threadHolder;
	travelDirectoryArgs->directory = directory;
	travelDirectoryArgs->directory_path = directory_path;

	return travelDirectoryArgs;
}

args_watchConnection * createThreadsWatchConnection(int *fdptr, ) {
	args_watchConnection watchConnectionArgs = (args_watchConnection *)malloc(sizeof(args_watchConnection));
	watchConnectionArgs->fdptr = fdptr;

	return watchConnectionArgs;
}

//If it already contains the phrase -sorted-SOMEVALIDCOLUMN.csv then the file is already sorted
//Returns 0 if the file has not yet been sorted
//Returns 1 if the file has been sorted
int isAlreadySorted(char *pathname,char *column_to_sort) {
	char *compareString = (char *) malloc(strlen("-sorted-") + strlen(column_to_sort) + strlen(".csv") + 1);
	//build the -sorted-SOMEVALIDCOLUMN.csv string
	strcpy(compareString, "-sorted-");
	strcat(compareString, column_to_sort);
	strcat(compareString, ".csv");
	if(strstr(pathname,compareString) == NULL) {
		free(compareString);		
		return 0;
	} else {
		free(compareString);
		return 1;		
	}
}
