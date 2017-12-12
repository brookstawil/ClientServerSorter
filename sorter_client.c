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
#include <semaphore.h>
#include "sorter_client.h"

#define SOD "SOD"
#define EOD "EOD" //End Of Document
#define SOC "SOC"
#define EOC "EOC" //End of Client
#define DMP "DMP" //Dump Request
#define SRT "SRT"

#define MAX_PATH_LENGTH 256

#define MAX_THREADS 4096
#define BUFF_SIZE 8192
#define MSG_SIZE 4096

#define PORT 8090

//the root is now an initial thread process
pid_t root;
pthread_t rootTID;
char *global_output_dir;
char *global_starting_dir;
char *global_column_to_sort;
char *global_serverAddress;
char *global_portNum;
char *header;
sem_t client_pool;
int lineCount;

int main (int argc, char* argv[]) {

	//We want to ensure that only the root thread does certain operations
	root = getpid();
	rootTID = pthread_self();

	int i;
	char *semaphore_size; 

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
		} else if(strcmp(argument, "-s")){
			semaphore_size = argv[i+1];
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
		printf("ERROR: No port number specified. Port number must be given with argument tag '-p'.\n");
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

	if(semaphore_size == NULL) {
		printf("Client socket pool amount is not specified, defaulting to '10' as the semaphore_size.\n");
		semaphore_size = "10";
	}

	sem_init(&client_pool, 0, atoi(semaphore_size));

	travdir(global_starting_dir, global_column_to_sort, global_output_dir);	

	return 0;
}

//A sendFileData thread will have the socket file descriptor (and other information?) passed in an arguments struct
void* sendFileData(void* args) 
{
	/*
		We must transmit:
			1. The line itself
			2. Some ending signal once the file is done
	*/
	args_sendFileData* sendFileDataArgs = args;

     /* set buffer and size to 0 they will be changed by getline */
	char *headerline = NULL;
	size_t size = 0;
	int sd = *(sendFileDataArgs->fdptr);
	lineCount = 0;

	ssize_t headerlineSize = getline(&headerline, &size, sendFileDataArgs->csvFile);

	/*
		getline() eats the newline character
	*/

	//Server is specifying forr the header line
	doSend(sd, headerline);	
	
	// Read each line
	// The existing line will be re-used, or, if necessary,
	// It will be `free`'d and a new larger buffer will `malloc`'d
	// I heard that fgets is better so I want to see if this works
	char line[1024];

    while (fgets(line, 1024, sendFileDataArgs->csvFile))
    {
    	size_t size = 0;
    	ssize_t lineSize = getline(&line, &size, sendFileDataArgs->csvFile);
		printf("Number of bytes of the line: %s\n", lineSize);
		if(lineSize != -1){
			// Discard newline character if it is present,
			if (lineSize > 0 && line[lineSize-1] == '\n') {
			    line[lineSize-1] = '\0';
			}
			
			//write to the server the size of the line
			//send(sd, lineSize, sizeof(lineSize), 0);

			//write to the server the actual line data
			doSend(sd, line);
	        printf("After the send function of the line \n");
	        lineCount++;
		} else {
			printf("ERROR: getline() call failed.\n");
		}
		// NOTE strtok clobbers tmp
    }

    //write that we are done reading the lines
	doSend(sd, EOD);

    //write the column to sort on and end signal
    char column[30];
    sprintf(column, "%s", global_column_to_sort);
    printf("column to sort are we doing this correctly %s \n", column);
    doSend(sd, column);

	//Now write the EOC signal
	doSend(sd, EOC);

	close(sd);
}

//The recieveFileData thread works very much like the watchConnection thread implemented above.
//However instead of just waiting for a terminating string, the thread also takes data given back from the server.
void* receiveAndWriteFileData(void* args) {
	args_receiveAndWriteFileData* receiveAndWriteFileData = args;

	/*
		This is essentially the reverse of what the server side is doing.
		The server should be sending lines over the socket, while the client reads in and interprets the bytes.
	*/

	//PSEUCODE:
			/*
				The server uses the send function back to the client 4 times in this order:
				1. doSend(sockFD, header) -> which is unnecessary but like okay we can take the headerline
				2. doSend(sockFd, rows) -> which is the rows in a char[] array
				3. doSend(sockFd, EOD) -> which just sends us EOD to signal they are done sending us the char[] array
				4. doSend(sockFd, EOC) -> which is unnecessary buy like okay we can take the column?
Idea: We can do a switch statement and have an int that holds 1-4 holding which data the server is sending through the socket
			*/

	int sd, i, csvFileOut;
	int bytesRead = 0;
	int lineCount = 0;
	int state = 0;
	char buffer[MSG_SIZE];
	char output[MSG_SIZE];

	sd = *(receiveAndWriteFileData->fdptr);
	csvFileOut = receiveAndWriteFileData->csvFileOut;
	
	while(state < 2)
	{	

        // read the data
        memset(buffer, '\0', sizeof(buffer));
        bytesRead = doRead(sd, buffer);
        if (bytesRead == 0) {
            printf("Exiting after 0 bytes read\n");
            break;
        }

        printf("line: %d read %d bytes %s\n\n", lineCount, bytesRead, buffer);
        doUnTrim(buffer, bytesRead);

		switch(state) {
			
			case 0: //Read data from the socket
				if(bytesRead == 3 && strncmp(buffer, EOD, 3)) {
                  	printf("Got EOD \n");
					fflush(stdout);
                    state = 2;
				} 
				else 
				{
					if (lineCount == 0 && header == NULL) {
						/*
							* No need to store the header line, just spit out to the file
						*/
						header = calloc(bytesRead+1, sizeof(char *));
						memcpy(header, buffer, bytesRead);
						write(csvFileOut, buffer, strlen(buffer));
					}

					char *line = calloc(bytesRead + 1, sizeof(char *));
					memcpy(line, buffer, bytesRead + 1);

					write(csvFileOut, buffer, strlen(buffer));
					lineCount++;
				}	
				break;

			default:
                fprintf(stderr, "Unknown state %d input %s\n", state, buffer);
				break;
		}
	}

	pthread_exit(NULL);
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
	char *directory_path = (char *) malloc(MAX_PATH_LENGTH);
	strcpy(directory_path, input_dir_path);
	
	DIR * directory = opendir(directory_path);

	if (directory == NULL) {
        return 1;
	}

	//have one thread go through directories
	int numThreads = MAX_THREADS;
	pthread_t* threadHolder = (pthread_t*)(malloc(sizeof(pthread_t) * numThreads));
	goThroughPath(createThreadsTraverse(output_dir, threadHolder, directory, directory_path));

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

	char message[256];

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
		//end of file stream, break->now wait on children
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
				pthread_create(&thread, 0, goThroughPath, createThreadsTraverse(output_dir, threadHolder, newDirectory, newDirectoryPath));
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
							1. The line itself
							2. Some ending signal once the file is done
						This is to be implemented in the sendFileData() thread function described above.
					*/

					/*
						If we want to accomplish the first extra credit, we would create some semaphore in main, using the -s parameter.
						And ANYWHERE we call createSockte, we look at this semaphore to check and see whether we have already made a certain number of connections.
						Most likely we would check the semaphore here and then call this conditional just below.
						sem_wait() here and then sem_post() after closing the socket with close(sd)
					*/

					sem_wait(&client_pool);

					int* sd;
					if ( (sd = createSocket( global_serverAddress, global_portNum )) == -1 ) {
						write( 1, message, sprintf( message,  "\x1b[1;31mCould not connect to server %s errno %s\x1b[0m\n", global_serverAddress, strerror( errno ) ) );
						return 1;
					} else {
						//With a valid file descriptor we can then split off into another thread which will deal with parsing the file and writing to the socket.
						printf( "Connected to server %s\n", global_serverAddress);
						int *fdptr = (int *)malloc(sizeof(int));
						*fdptr = sd;

						//Once we have the socket file pointer we can create a new thread, call it sendFileData
						//TODO: create some arg structure for this thread, we need at least the fdptr, what else do we need?
						pthread_t sendDumpFileData_thread; // watchConnection_thread;
						if(pthread_create(&sendDumpFileData_thread, NULL, sendFileData, createThreadsSendFileData(fdptr, csvFile)) != 0){
							printf( "pthread_create() failed in file %s line %d\n", __FILE__, __LINE__ );
							return 0;
						}

						/*
							In this implementation the client will join connection threads just after creation. 
							Meaning that traversing to the next file in the directory will not happen until the file has been sent.
							Do we need to join or can we just let these threads run?
							This is going based off the decription that Tjang gave in the assignment page.
						*/

						pthread_join(sendDumpFileData_thread, NULL);

						sendDumpFileData_thread = NULL;

						free(fdptr);
						close(sd);

						sem_post(&client_pool);
						return 1; 
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

	//Join the directory threads to finish 
	int i=0,j=0;
	for(i = 0; i < travelDirectoryArgs->counter; i++){
		pthread_join(travelDirectoryArgs->threadHolder[i], NULL);
	}

	free(travelDirectoryArgs->directory);

	/*	
		At this point in execution any given directory thread should have finished with its children.
		Only the root thread should be requesting for the sorted files, otherwise let the individual directory threads exit.
	*/

	//Anything that occurs in this conditional will only be done by the root thread, otherwise the individual directory threads will skip this and exit.
	if(getpid() == root && pthread_self() == rootTID)
	{

		/*
			If we wish to implement the extra credit then we would do as we did above and sem_wait().
			sem_post would occur below.
			We do this as we are attempting to create a socket again over here, and thus need to check if we still have socket counter left in the pool.
		*/

		sem_wait(&client_pool);

		int* sd;
		if ( (sd = createSocket( global_serverAddress, global_portNum )) == -1 ) {
			write( 1, message, sprintf( message,  "\x1b[1;31mCould not connect to server %s errno %s\x1b[0m\n", global_serverAddress, strerror( errno ) ) );
			return 1;
		} else {
			
			//First we must create a local FILE pointer to write the output to
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
			
			//We now pass this FILE pointer as an argument to the recieveFileData thread

			pthread_t receiveAndWriteFileData_thread;
			if(pthread_create(&receiveAndWriteFileData_thread, NULL, receiveAndWriteFileData, createThreadsReceiveAndWriteFileData(sd, csvFileOut) != 0)){
				printf( "pthread_create() failed in file %s line %d\n", __FILE__, __LINE__ );
				return 0;
			}

			pthread_join(receiveAndWriteFileData_thread, NULL);
			close(sd);

		}

		sem_post(&client_pool);
		sem_destroy(&client_pool);
		free(finalDirectoryPath);
		free(args);
		exit(0);
	}

	pthread_exit(NULL);
}

//Helper function that sets the arguments for a thread that transmits a given file 
args_sendFileData * createThreadsSendFileData(int *fdptr, int csvFile) {
	args_sendFileData* sendFileDataArgs = malloc(sizeof(args_sendFileData));
	sendFileDataArgs->fdptr = fdptr;
	sendFileDataArgs->csvFile = csvFile;

	return sendFileDataArgs;
}

args_travelDirectory * createThreadsTraverse(char * output_dir, pthread_t* threadHolder, DIR * directory, char *directory_path){
	args_travelDirectory* travelDirectoryArgs = malloc(sizeof(args_travelDirectory));
	travelDirectoryArgs->output_dir = output_dir;
	travelDirectoryArgs->threadHolder = threadHolder;
	travelDirectoryArgs->directory = directory;
	travelDirectoryArgs->directory_path = directory_path;

	return travelDirectoryArgs;
}

args_receiveAndWriteFileData * createThreadsReceiveAndWriteFileData(int *fdptr, FILE* csvFileOut) {
	args_receiveAndWriteFileData* receiveAndWriteFileDataArgs = malloc(sizeof(args_receiveAndWriteFileData));
	receiveAndWriteFileDataArgs->fdptr = fdptr;
	receiveAndWriteFileDataArgs->csvFileOut = csvFileOut;

	return receiveAndWriteFileDataArgs;
}

//Helper Function that sends a message over a socket.
int doSend(int sockFd, char *msg) {

    char buffer[2048] = {0};

    int len = strlen(msg);

    memcpy(buffer, &len, sizeof(int));
    memcpy(buffer + sizeof(int), msg, len);
    //buffer is all of the rows and the message EOD at the end 
    return send(sockFd, buffer, len+ sizeof(int), 0);
}

//Helper function that reads a message over a socket
int doRead(int sockFd, char *buffer) {

    int len;
    int bytesRead = read(sockFd, &len, sizeof(int));

    printf("read %d bytes msg length %d\n", bytesRead, len);

    bytesRead = read(sockFd, buffer, len);

    printf("read %d bytes msg %s\n", bytesRead, buffer);

    return bytesRead;
}

//Helper function that cleans up for newline charcters
void doUnTrim(char input[], size_t size) {
    if (input[size-1]=='\0'){
        input[size-1]='\n';
		input[size]='\0';
    }
	/*
	//Do we need the /r character?
    if (input[size-2]=='\r'){
        input[size-2]='\0';
    }
	*/
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
				