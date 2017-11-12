/* 
 * CS 241
 * The University of Illinois
 */

#include <stdio.h>
#include <stdlib.h>
#include <sys/select.h>
#include <pthread.h>
#include <string.h>
#include <assert.h>
#include <unistd.h>
#include <sys/wait.h>
#include <poll.h>

#include "libmapreduce.h"
#include "libdictionary.h"

static const int BUFFER_SIZE = 2048;


static void process_key_value(const char *key, const char *value, mapreduce_t *mr)
{
    if (dictionary_add(&mr->dict,key,value) == KEY_EXISTS) {
        const char *oldValue = dictionary_get(&mr->dict,key);
        const char *newValue = mr->reducefunc(oldValue,value);
        dictionary_remove(&mr->dict,key);
        dictionary_add(&mr->dict,key,newValue);
    }
}


static int read_from_fd(int fd, char *buffer, mapreduce_t *mr)
{
	/* Find the end of the string. */
	int offset = strlen(buffer);

	/* Read bytes from the underlying stream. */
	int bytes_read = read(fd, buffer + offset, BUFFER_SIZE - offset);
	if (bytes_read == 0){
		return 0;
	}
	if(bytes_read < 0){
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

		//assert(split != NULL);

		/* Allocate and assign memory */
		char *key = malloc((split - buffer + 1) * sizeof(char));
		char *value = malloc((strlen(split) - 2 + 1) * sizeof(char));

		strncpy(key, buffer, split - buffer);
		key[split - buffer] = '\0';

		strcpy(value, split + 2);

		/* Process the key/value. */
		process_key_value(key, value, mr);

		/* Shift the contents of the buffer to remove the space used by the processed line. */
		memmove(buffer, line + 1, BUFFER_SIZE - ((line + 1) - buffer));
		buffer[BUFFER_SIZE - ((line + 1) - buffer)] = '\0';
	}
	return 1;
}

void mapreduce_init(mapreduce_t *mr, 
			void (*mymap)(int, const char *), 
				const char *(*myreduce)(const char *, const char *))
{	
    mr->mapfunc = mymap;
    mr->reducefunc = myreduce;
    dictionary_init(&mr->dict);
}

typedef struct {
    int *fds;
    int fds_num;
    mapreduce_t *mr;
} worker_func_args;

void worker_func(void *arg) {
    int *fds,fds_num,res,i,max_fds,read_count;
    mapreduce_t *mr;
    worker_func_args *args;
    fd_set read_set;
    char *buf;
    struct timeval tv;
    tv.tv_sec = 0;
    tv.tv_usec = 0;

    //unpack args
    args = (worker_func_args*)arg;
    fds = args->fds;
    fds_num = args->fds_num;
    mr = args->mr;
    //prepare the buf and read_set
    buf = (char*)malloc(sizeof(char)*(BUFFER_SIZE+1));
    if (buf == NULL) {
        perror("malloc");
        exit(-1);
    }

    FD_ZERO(&read_set);
    read_count = 0;
    max_fds = fds[(fds_num)*2-1] + 1;
    while(read_count != fds_num) {
        for(i = 0;i < fds_num;i++)
            if (FD_ISSET(fds[2*i],&read_set)) {
                read_from_fd(fds[2*i],buf,mr);
                buf[0] = '\0';
                read_count++;
            } else {
                FD_SET(fds[2*i],&read_set);
            }

        res = select(max_fds,&read_set,NULL,NULL,&tv);
        if (res == -1) {
            perror("select");
            exit(-1);
        }
    }
    free(buf);
    free(fds);
    free(args);
    pthread_exit(NULL);
}

void mapreduce_map_all(mapreduce_t *mr, const char **values)
{
    int value_num = 0,res,i;
    int *fds;
    worker_func_args *args;
    //get value count
    while(values[value_num] != NULL) value_num++;
    
    //new fds and pipe
    //read port fds[2*i], write port fds[2*i+1]
    fds = (int*)malloc(sizeof(int)*value_num*2);
    if (fds == NULL) {
        perror("malloc");
        exit(-1);
    }
    for(i = 0;i < value_num;i++)
        if (pipe(&fds[2*i]) == -1) {
            perror("pipe");
            exit(-1);
        }
    
    //fork and map
    for(i = 0;i < value_num;i++) {
        res = fork();
        if (res == -1) { perror("fork"); exit(-2); }
        else if (res == 0) {
            //child
            mr->mapfunc(fds[2*i+1],values[i]);
            exit(0);
        }
    }

    //construct the args
    args = (worker_func_args*)malloc(sizeof(worker_func_args));
    if (args == NULL) {
        perror("malloc");
        exit(-1);
    }
    args->fds = fds;
    args->fds_num = value_num;
    args->mr = mr;

    //new pthread
    res = pthread_create(&mr->worker,NULL,(void *)worker_func,(void*)args);
}

void mapreduce_reduce_all(mapreduce_t *mr)
{
    pthread_join(mr->worker,NULL);
}

const char *mapreduce_get_value(mapreduce_t *mr, const char *result_key)
{
	return dictionary_get(&mr->dict,result_key);
}

void mapreduce_destroy(mapreduce_t *mr)
{

}

