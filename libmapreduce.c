/** @file libmapreduce.c */
/* 
 * CS 241
 * The University of Illinois
 */

#include <stdio.h>
#include <stdlib.h>
#include <sys/epoll.h>
#include <pthread.h>
#include <string.h>
#include <assert.h>
#include <unistd.h>
#include <sys/wait.h>

#include "libmapreduce.h"
#include "libds/libds.h"


static const int BUFFER_SIZE = 2048;  /**< Size of the buffer used by read_from_fd(). */

pthread_t *worker;

/**
 * Adds the key-value pair to the mapreduce data structure.  This may
 * require a reduce() operation.
 *
 * @param key
 *    The key of the key-value pair.  The key has been malloc()'d by
 *    read_from_fd() and must be free()'d by you at some point.
 * @param value
 *    The value of the key-value pair.  The value has been malloc()'d
 *    by read_from_fd() and must be free()'d by you at some point.
 * @param mr
 *    The pass-through mapreduce data structure (from read_from_fd()).
 */
static void process_key_value(const char *key, const char *value, mapreduce_t *mr)
{
	int i;
	for(i=0;i<mr->size; i++){
		if(strcmp(mr->entries[i]->key, key)==0){
			char *ret = mr->myreduce(mr->entries[i]->value, value);
			if(strlen(mr->entries[i]->value)< strlen(ret)) mr->entries[i]->value = realloc(mr->entries[i]->value, strlen(ret)+1);
			strcpy(mr->entries[i]->value, ret);
			free(ret);
			return;
		}
	}

	if(mr->size >= mr->eSize) mr->entries = realloc(mr->entries, (mr->eSize *= 2) * sizeof(entry_t*)); 
	mr->entries[mr->size] = (entry_t *)malloc(sizeof(entry_t));
	mr->entries[mr->size]->key = malloc(strlen(key)+1);
	mr->entries[mr->size]->value = malloc(strlen(value)+1);
	strcpy(mr->entries[mr->size]->key, key);
	strcpy(mr->entries[mr->size]->value, value);
	mr->size++;

}


/**
 * Helper function.  Reads up to BUFFER_SIZE from a file descriptor into a
 * buffer and calls process_key_value() when for each and every key-value
 * pair that is read from the file descriptor.
 *
 * Each key-value must be in a "Key: Value" format, identical to MP1, and
 * each pair must be terminated by a newline ('\n').
 *
 * Each unique file descriptor must have a unique buffer and the buffer
 * must be of size (BUFFER_SIZE + 1).  Therefore, if you have two
 * unique file descriptors, you must have two buffers that each have
 * been malloc()'d to size (BUFFER_SIZE + 1).
 *
 * Note that read_from_fd() makes a read() call and will block if the
 * fd does not have data ready to be read.  This function is complete
 * and does not need to be modified as part of this MP.
 *
 * @param fd
 *    File descriptor to read from.
 * @param buffer
 *    A unique buffer associated with the fd.  This buffer may have
 *    a partial key-value pair between calls to read_from_fd() and
 *    must not be modified outside the context of read_from_fd().
 * @param mr
 *    Pass-through mapreduce_t structure (to process_key_value()).
 *
 * @retval 1
 *    Data was available and was read successfully.
 * @retval 0
 *    The file descriptor fd has been closed, no more data to read.
 * @retval -1
 *    The call to read() produced an error.
 */
static int read_from_fd(int fd, char *buffer, mapreduce_t *mr)
{
	/* Find the end of the string. */
	int offset = strlen(buffer);

	/* Read bytes from the underlying stream. */
	int bytes_read = read(fd, buffer + offset, BUFFER_SIZE - offset);
	if (bytes_read == 0)
		return 0;
	else if(bytes_read < 0)
	{
		fprintf(stderr, "error in read.\n");
		return -1;
	}

	buffer[offset + bytes_read] = '\0';

	/* Loop through each "key: value\n" line from the fd. */
	char *line;
	while ((line = strstr(buffer, "\n")) != NULL)
	{
		*line = '\0';

		/* Find the key/value split. */
		char *split = strstr(buffer, ": ");
		if (split == NULL)
			continue;

		/* Allocate and assign memory */
		char *key = malloc((split - buffer + 1) * sizeof(char));
		char *value = malloc((strlen(split) - 2 + 1) * sizeof(char));

		strncpy(key, buffer, split - buffer);
		key[split - buffer] = '\0';

		strcpy(value, split + 2);

		/* Process the key/value. */
		process_key_value(key, value, mr);

		free(key);
		free(value);
		/* Shift the contents of the buffer to remove the space used by the processed line. */
		memmove(buffer, line + 1, BUFFER_SIZE - ((line + 1) - buffer));
		buffer[BUFFER_SIZE - ((line + 1) - buffer)] = '\0';
	}

	return 1;
}

void *retrieve(void* atr){
	mapreduce_t *mr = (mapreduce_t*)atr;
	int counter = mr->length;
	while(counter>0){
		struct  epoll_event ev;
		bzero(&ev, sizeof(struct epoll_event));
    	epoll_wait( mr->fd, &ev, 1, -1);
    	int i;
    	char *buf = NULL;
    	for(i=0;i<mr->length;i++){
    		if(mr->fds[i]->fd!=NULL && mr->fds[i]->fd[0] == ev.data.fd)
    			buf = mr->fds[i]->value;
    	}

		int suc = read_from_fd(ev.data.fd, buf , mr);
		if(suc != 1 ){
			--counter;
			epoll_ctl(mr->fd, EPOLL_CTL_DEL, ev.data.fd, NULL);
		}
	}


	return (void *)mr;
}


/**
 * Initialize the mapreduce data structure, given a map and a reduce
 * function pointer.
 */
void mapreduce_init(mapreduce_t *mr, 
                    void (*mymap)(int, const char *), 
                    const char *(*myreduce)(const char *, const char *))
{	
	mr->mymap = mymap;
	mr->myreduce = myreduce;
	mr->size = 0;
	mr->length = 0;
	mr->eSize = 4;
	mr->entries = (entry_t **)malloc(sizeof(entry_t*) * mr->eSize);
}


/**
 * Starts the map() processes for each value in the values array.
 * (See the MP description for full details.)
 */
void mapreduce_map_all(mapreduce_t *mr, const char **values)
{
	/*transition fds to the mapreduce object and change how we store entries.
	create a set for the fds linking to their buffers. */
	//create fds and entry objects
	int i;
	for(i=0;values[i]!=NULL;i++);
	mr->length = i;
	mr->fds = (entry_t **)malloc(sizeof(entry_t*) * mr->length);

	/* create epoll_fd and epoll_events */
    int epoll_fd = epoll_create(10);
    struct epoll_event events[mr->length];
    
	for(i=0;i<mr->length;i++){
		mr->fds[i] = (entry_t *)malloc(sizeof(entry_t));
		mr->fds[i]->fd = malloc(2 * sizeof(int));
		mr->fds[i]->value = malloc(BUFFER_SIZE+1);
		mr->fds[i]->value[0] = '\0';
		pipe(mr->fds[i]->fd);
		//printf("call::::::%d\n", i);
		int pid = fork();
		int read_fd = mr->fds[i]->fd[0];
        int write_fd = mr->fds[i]->fd[1];
		if(pid == 0){ //child
			//int j;
            //for(j=0;j<=i;j++) close(mr->fds[j]->fd[0]);
            //for(j=0;j< i;j++) close(mr->fds[j]->fd[1]);

            mr->mymap(write_fd, values[i]);

        	close(write_fd);

        	bzero(&(events[i]), sizeof(struct epoll_event));
        	events[i].events = EPOLLIN;
        	events[i].data.fd = read_fd;
        	epoll_ctl(epoll_fd, EPOLL_CTL_ADD, read_fd, &events[i]);

        	exit(0);
		}else{ //parent
			close(write_fd);
		}
	}

	mr->fd = epoll_fd;
	worker = malloc(sizeof(pthread_t));
	pthread_create(worker, NULL, retrieve, (void *)mr);
}


/**
 * Blocks until all the reduce() operations have been completed.
 * (See the MP description for full details.)
 */
void mapreduce_reduce_all(mapreduce_t *mr)
{
	void *ret;
	pthread_join(*worker, &ret);


}


/**
 * Gets the current value for a key.
 * (See the MP description for full details.)
 */
const char *mapreduce_get_value(mapreduce_t *mr, const char *result_key)
{
	int i;
	char *ret = NULL;
	for(i=0;i<mr->size;i++){
		if(strcmp(mr->entries[i]->key, result_key) ==0){
			ret = malloc(strlen(mr->entries[i]->value)+1);
			strcpy(ret, mr->entries[i]->value);
			return ret;
		}
	}
	return ret;
}


/**
 * Destroys the mapreduce data structure.
 */
void mapreduce_destroy(mapreduce_t *mr)
{
	int i;
	for(i=0;i<mr->size; i++){
		free(mr->entries[i]->key);
		free(mr->entries[i]->value);
		free(mr->entries[i]);
	}

	for(i=0;i<mr->length;i++){
		free(mr->fds[i]->fd);
		free(mr->fds[i]->value);
		free(mr->fds[i]);
	}

	free(mr->entries);
	free(mr->fds);
	free(worker);
}
