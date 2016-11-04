/*
 *  general_utility.h
 *  
 *
 *  Created by esma yildirim on 10/10/14.
 *  Copyright 2014 __MyCompanyName__. All rights reserved.
 *
 */

#define FILENAME_LEN 100 //chars
#define PATH_LEN 500 //chars
#define SIZE 100
#define FTP_NUM_PIPLINED_FILES 5000

typedef struct globus_file_s
{
    char *	file_inner_path;
    long	file_size;
    char *	full_path;
    char *	dest_path;
    int     opt_pp;
    char	file_type[5];
} globus_file_t;
typedef struct globus_l_gridftp_test_url_pairs_s
{
    char **                             source_array;
    char **                             dest_array;
    int                                 index;
    int                                 count;
} globus_l_gridftp_test_url_pairs_t;

#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <semaphore.h>
#include "globus_ftp_client.h"

void init_string(char *string, int size);
globus_file_t * create_full_element(char * inner_path, char * full_path, char * dest_path, long file_size, char * file_type);
globus_file_t * create_element();
globus_file_t * enqueue(globus_file_t * main_list, int * main_size, globus_file_t * element );
globus_file_t * dequeue(globus_file_t * main_list, int * main_size, globus_file_t * element);
globus_file_t * combine_lists(globus_file_t * list1, int *size1, globus_file_t *list2, int size2);
globus_file_t * split_list(globus_file_t * list1,
                           int *size1,
                           globus_file_t * list2,
                           int *size2);
void quick_sort_by_size( globus_file_t *list, int left, int right);
int partition( globus_file_t * list, int left, int right);
long getsize(globus_file_t *list, int no_of_files);



void convert_byte_to_string(char char_array[],char ** string_array, int *count);
globus_file_t* list_files(char *src_url,
                          char *dst_url,
                          globus_file_t *file_array,
                          int * no_of_files);
void get_time_fun(double * tdouble, time_t * time_secs);
int perform_transfer(globus_file_t * file_array,
					 int no_of_files,
					 long chunk_size, 
					 int ppq, 
					 int p, 
					 int cc,  
					 double *thr);
int perform_multichunk_transfer(globus_file_t ** chunk_array, 
								int no_of_replicas, 
								int total_no_files,
								int * no_files_per_chunk, 
								long total_size, 
								int ppq, 
								int p, 
								int cc_per_chunk, 
								double *thr);
