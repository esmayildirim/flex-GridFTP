//
//  general_utility.c
//  
//
//  Created by esma yildirim on 11/02/16.
//  Copyright (c) 2016 __Rutgers University__. All rights reserved.
//

#include "general_utility.h"

static globus_mutex_t lock;
static globus_cond_t cond;
static globus_bool_t done;
static globus_bool_t error = GLOBUS_FALSE;
int handle_count ;
int *handle_multichunk_count;
static char    char_buffer[2000 * FTP_NUM_PIPLINED_FILES];
static int count = 0;


void reset_charbuffer()
{

	int i = 0; 
	for(i = 0; i< 2000*FTP_NUM_PIPLINED_FILES;i++)
		char_buffer[i] = '\0';
    count = 0;
}
void init_string(char *string, int size)
{
	int i= 0;
	for(i = 0 ; i< size; i++)
		string[i] = '\0';

}

long getsize(globus_file_t *list, int no_of_files)
{
	int i;
	long total_size = 0;
	for (i=0; i<no_of_files; i++) {
		total_size += list[i].file_size;
	}
	return total_size;
}

globus_file_t * create_full_element(char * inner_path, char * full_path, char * dest_path, long file_size, char * file_type)
{
	globus_file_t * element = (globus_file_t *)malloc(sizeof(globus_file_t));
	element->file_inner_path = (char *)malloc(FILENAME_LEN* sizeof(char));
	init_string(element->file_inner_path, FILENAME_LEN);
	strcpy(element->file_inner_path, inner_path);
	
	element->full_path = (char *)malloc(PATH_LEN* sizeof(char));
	init_string(element->full_path, PATH_LEN);
	strcpy(element->full_path, full_path);
	
	element->dest_path = (char *)malloc(PATH_LEN* sizeof(char));
	init_string(element->dest_path, PATH_LEN);
	strcpy(element->dest_path, dest_path);
	
	element->file_size = file_size;
	strcpy(element->file_type, file_type);
    return element;

}

globus_file_t * create_element()
{
	globus_file_t * element = (globus_file_t *)malloc(sizeof(globus_file_t));
	element->file_inner_path = (char *)malloc(FILENAME_LEN* sizeof(char));
	init_string(element->file_inner_path, FILENAME_LEN);
		
	element->full_path = (char *)malloc(PATH_LEN* sizeof(char));
	init_string(element->full_path, PATH_LEN);
		
	element->dest_path = (char *)malloc(PATH_LEN* sizeof(char));
	init_string(element->dest_path, PATH_LEN);
		
    init_string(element->file_type,5);
    return element;
	
}



globus_file_t * enqueue(globus_file_t * main_list, int * main_size, globus_file_t * element )
{
    main_list = (globus_file_t *)realloc(main_list, sizeof(globus_file_t)*(*main_size+1));
    main_list[*main_size].file_inner_path = malloc(sizeof(char)*FILENAME_LEN);
	init_string(main_list[*main_size].file_inner_path, FILENAME_LEN);
	
    main_list[*main_size].full_path = malloc(sizeof(char)*PATH_LEN);
    init_string(main_list[*main_size].full_path, PATH_LEN);
	
	main_list[*main_size].dest_path = malloc(sizeof(char)*PATH_LEN);
    init_string(main_list[*main_size].dest_path, PATH_LEN);
	strncpy(main_list[*main_size].file_inner_path, element->file_inner_path,FILENAME_LEN-1);
    strncpy(main_list[*main_size].full_path, element->full_path,PATH_LEN-1);
    strncpy(main_list[*main_size].dest_path, element->dest_path,PATH_LEN-1);
	strcpy(main_list[*main_size].file_type, element->file_type);

    main_list[*main_size].file_size = element->file_size;
    (*main_size)++;
	return main_list;

}

globus_file_t * dequeue(globus_file_t * main_list, int * main_size, globus_file_t * element)
{
    element = create_element();
	strncpy(element->file_inner_path, main_list[*main_size-1].file_inner_path,FILENAME_LEN-1);
    strncpy(element->full_path, main_list[*main_size-1].full_path,PATH_LEN-1);
    strncpy(element->dest_path, main_list[*main_size-1].dest_path,PATH_LEN-1);
    element->file_size = main_list[*main_size-1].file_size;
	strcpy(element->file_type,main_list[*main_size-1].file_type);
    main_list = (globus_file_t *) realloc (main_list, sizeof(globus_file_t)*(*main_size -1));
	(*main_size)--;
    return element;
}

globus_file_t * combine_lists(globus_file_t * list1, int *size1, globus_file_t *list2, int size2)
{
    int i,j = 0;
    list1 = (globus_file_t *)realloc(list1, (*size1 + size2)*sizeof(globus_file_t));
	printf("size1 %d size2 %d\n",*size1, size2);
	
    for(i= *size1; i <(*size1+size2); i++)
    {
        list1[i].file_inner_path = (char *)malloc(sizeof(char)*FILENAME_LEN);
		init_string(list1[i].file_inner_path, FILENAME_LEN);
		strncpy(list1[i].file_inner_path, list2[j].file_inner_path,FILENAME_LEN-1);
		
		list1[i].full_path = (char *)malloc(sizeof(char)*PATH_LEN);
		init_string(list1[i].full_path, PATH_LEN);
        strncpy(list1[i].full_path, list2[j].full_path,PATH_LEN-1);
		
		list1[i].dest_path = (char *)malloc(sizeof(char)*PATH_LEN);
        init_string(list1[i].dest_path, PATH_LEN);
		strncpy(list1[i].dest_path, list2[j].dest_path,PATH_LEN-1);
        list1[i].file_size = list2[j].file_size;
		strncpy(list1[i].file_type, list2[j].file_type,4);
        j++;
    }
    *size1 = *size1 +size2;
	
	return list1;
}

/*
* For a given list1, the function splits it and makes a new list with the indicated
* size. 
* Both list1 is not reallocated only its size value size1 is updated after the split.
* The new list contains elements from the end of list1 
*/
globus_file_t * split_list(globus_file_t * list1, int *size1, globus_file_t * list2, int *size2)
{
    if(*size1<*size2)
        *size2 = *size1;
    list2 = (globus_file_t *)malloc(*size2 * sizeof(globus_file_t));
    int j,i;
    j = *size1 - *size2;
    
    for(i = 0; i< *size2 ;i++,j++)
    {
        list2[i].file_inner_path = (char *)malloc(sizeof(char)*FILENAME_LEN);
        init_string(list2[i].file_inner_path, FILENAME_LEN);
        strncpy(list2[i].file_inner_path, list1[j].file_inner_path, FILENAME_LEN-1);
        
        list2[i].full_path = (char *)malloc(sizeof(char)*PATH_LEN);
        init_string(list2[i].full_path,PATH_LEN);
        strncpy(list2[i].full_path, list1[j].full_path, PATH_LEN-1);
        
        list2[i].dest_path = (char *)malloc(sizeof(char)*PATH_LEN);
        init_string(list2[i].dest_path,PATH_LEN);
        strncpy(list2[i].dest_path, list1[j].dest_path, PATH_LEN-1);
        list2[i].file_size = list1[j].file_size;
        init_string(list2[i].file_type,5);
        strncpy(list2[i].file_type, list1[j].file_type,4);
        
    }
    
    *size1 = *size1 - *size2;
    return list2;
    
    
    
}

/* Applies a quick sort algorithm on a globus_file_t list structure based
*  on the size element
*/

void quick_sort_by_size( globus_file_t *list, int left, int right)
{
    int j;
    
    if( left < right )
    {
        // divide and conquer
        j = partition( list, left, right);
        quick_sort_by_size( list, left, j-1);
        quick_sort_by_size( list, j+1, right);
    }
	
}

int partition( globus_file_t * list, int left, int right) {
    int i, j;
    globus_file_t pivot, t;
    pivot.file_inner_path = (char *)malloc(sizeof(char)*FILENAME_LEN);
    pivot.full_path = (char *)malloc(sizeof(char)*PATH_LEN);
    pivot.dest_path = (char *)malloc(sizeof(char)*PATH_LEN);
    t.file_inner_path = (char *)malloc(sizeof(char)*FILENAME_LEN);
    t.full_path = (char *)malloc(sizeof(char)*PATH_LEN);
    t.dest_path = (char *)malloc(sizeof(char)*PATH_LEN);
    //printf("helloooo");
    strncpy(pivot.file_inner_path, list[left].file_inner_path, FILENAME_LEN-1);
    strncpy(pivot.full_path, list[left].full_path, PATH_LEN-1);
    strncpy(pivot.dest_path, list[left].dest_path, PATH_LEN-1);
    pivot.file_size = list[left].file_size;
	strncpy(pivot.file_type,list[left].file_type,4);
    i = left; j = right+1;
    
    while( 1)
    {
        do ++i; while( list[i].file_size <= pivot.file_size && i <= right );
        do --j; while( list[j].file_size > pivot.file_size );
        if( i >= j ) break;
        
        strncpy(t.file_inner_path, list[i].file_inner_path, FILENAME_LEN-1);
        strncpy(t.full_path, list[i].full_path, PATH_LEN-1);
        strncpy(t.dest_path, list[i].dest_path, PATH_LEN-1);
        t.file_size = list[i].file_size;
        strncpy(t.file_type,list[i].file_type,4);
        
        strncpy(list[i].file_inner_path, list[j].file_inner_path, FILENAME_LEN-1);
        strncpy(list[i].full_path, list[j].full_path, PATH_LEN-1);
        strncpy(list[i].dest_path, list[j].dest_path, PATH_LEN-1);
        list[i].file_size = list[j].file_size;
        strncpy(list[i].file_type,list[j].file_type,4);
		
        strncpy(list[j].file_inner_path, t.file_inner_path, FILENAME_LEN-1);
        strncpy(list[j].full_path, t.full_path, PATH_LEN-1);
        strncpy(list[j].dest_path, t.dest_path, PATH_LEN-1);
        list[j].file_size = t.file_size;
		strncpy(list[j].file_type,t.file_type,4);
    }
    
    strncpy(t.file_inner_path, list[left].file_inner_path, FILENAME_LEN-1);
    strncpy(t.full_path, list[left].full_path, PATH_LEN-1);
    strncpy(t.dest_path, list[left].dest_path, PATH_LEN-1);
    t.file_size = list[left].file_size;
    strncpy(t.file_type,list[left].file_type,4);
	
    strncpy(list[left].file_inner_path, list[j].file_inner_path, FILENAME_LEN-1);
    strncpy(list[left].full_path, list[j].full_path, PATH_LEN-1);
    strncpy(list[left].dest_path, list[j].dest_path, PATH_LEN-1);
    list[left].file_size = list[j].file_size;
    strncpy(list[left].file_type,list[j].file_type,4);
	
    strncpy(list[j].file_inner_path, t.file_inner_path, FILENAME_LEN-1);
    strncpy(list[j].full_path, t.full_path, PATH_LEN-1);
    strncpy(list[j].dest_path, t.dest_path, PATH_LEN-1);
    list[j].file_size = t.file_size;
	strncpy(list[j].file_type,t.file_type,4);
    return j;
}


/* indicates the file list is obtained */
static
void
done_cb(
        void *                                  user_arg,
        globus_ftp_client_handle_t *            handle,
        globus_object_t *                       err)
{
    char * tmpstr;
	
    if(err) tmpstr = " an";
    else    tmpstr = "out";
	
    if(err)
    { 
        printf("done_cb: file list retrieval done with%s error: %s\n", tmpstr, globus_object_printable_to_string(err)); 
        error++; 
    }
    else fprintf(stdout,"no problem in file list retrieval\n");
    globus_mutex_lock(&lock);
    done = GLOBUS_TRUE;
    globus_cond_signal(&cond);
    globus_mutex_unlock(&lock);
	
}
static
void
mkdir_done_cb(
		void *					user_arg,
		globus_ftp_client_handle_t *		handle,
		globus_object_t *			err)
{
    char * tmpstr;
	
    if(err) tmpstr = " an";
    else    tmpstr = "out";
	
    if(err) { printf("done with%s error\n", tmpstr);
		error = GLOBUS_TRUE; }
    globus_mutex_lock(&lock);
    done = GLOBUS_TRUE;
    globus_cond_signal(&cond);
    globus_mutex_unlock(&lock);
	
}


/*TO READ THE FILE INFORMATION FROM THE READ BUFFER*/
static
void
filelist_data_cb(
        void *					user_arg,
        globus_ftp_client_handle_t *		handle,
        globus_object_t *				err,
        globus_byte_t *				buffer,
        globus_size_t				length,
        globus_off_t				offset,
        globus_bool_t				eof)
{
    int j;
    //fwrite(buffer, 1, length, stdout);
    for(j=0; j<length; j++)
        char_buffer[count++]=(char)buffer[j];
	
	
    
    if(!eof)
    {
        globus_ftp_client_register_read(handle,
                                        buffer,
                                        SIZE,
                                        filelist_data_cb,
                                        0);
    }
	else {
		char_buffer[count++]='\0';
	}

}

void convert_byte_to_string(char char_array[],char ** string_array, int *count)
{
    char * result = NULL;
    int i=0;
    
    result = strtok(char_array, "\n");
    while(result!=NULL)
    {
		string_array[i] = malloc(2000 * sizeof(char *));
        strcpy(string_array[i],result);
		i++;
        result = strtok(NULL, "\n");
        
    }
    *count =i; //additional . and .. are removed from the list
    
}

/*
 * Contacts the gridftp server, gathers the file list for the given source path
 * prepares a globus_file_t list structure and returns it
 */

globus_file_t* list_files(char *src_url,char *dst_url, globus_file_t *file_array, int * no_of_files)
{
    globus_byte_t				buffer[SIZE];
    globus_size_t				buffer_length = SIZE;
    globus_ftp_client_handle_t                  handle;
    globus_ftp_client_operationattr_t           attr;
    globus_result_t                             result;
    globus_ftp_client_handleattr_t              handle_attr;
    globus_module_activate(GLOBUS_FTP_CLIENT_MODULE);
    
    globus_ftp_client_handleattr_init(&handle_attr);
    globus_ftp_client_operationattr_init(&attr);
    
    globus_mutex_init(&lock, GLOBUS_NULL);
    globus_cond_init(&cond, GLOBUS_NULL);
    globus_ftp_client_operationattr_set_type(&attr,
                                             GLOBUS_FTP_CONTROL_TYPE_ASCII);
    globus_ftp_client_handle_init(&handle, &handle_attr);
    done = GLOBUS_FALSE;
    result = globus_ftp_client_recursive_list(&handle,
                                            src_url,
                                            &attr,
                                            done_cb,
                                            0);
    if(result != GLOBUS_SUCCESS)
        done = GLOBUS_TRUE;
    else
    {
        globus_ftp_client_register_read(
                                        &handle,
                                        buffer,
                                        buffer_length,
                                        filelist_data_cb,
                                        0);
    }
    globus_mutex_lock(&lock);
    while(!done)
    {
        globus_cond_wait(&cond, &lock);
    }
    globus_mutex_unlock(&lock);
    globus_ftp_client_handle_destroy(&handle);
    globus_ftp_client_handleattr_destroy(&handle_attr);
    globus_ftp_client_operationattr_destroy(&attr);
    
    globus_mutex_destroy(&lock);
    globus_cond_destroy(&cond);
    
	char **my_string_array = globus_malloc(FTP_NUM_PIPLINED_FILES * sizeof(char *));
    int z,count_string;
    convert_byte_to_string(char_buffer,my_string_array,&count_string);
    
    file_array= (globus_file_t *)malloc((count_string ) * sizeof(globus_file_t)); // array of pointers of size `size`
    
    
    for(z=0; z<count_string ;z++)
    {
        
        char * result =NULL;
        int i= 0;
        char string_g[2000];
        strcpy(string_g, my_string_array[z]);
        result = strtok(string_g, "=; \t\n\r");
        
        while(result!=NULL)
        {
            //printf("%s\t",result);
            
			if(i==1)
            {
                strcpy(file_array[z].file_type,result);
				
            }
            else if(i==5)
            {   
				file_array[z].file_size = atol(result);
                //printf("CONV: %s filesize%ld\n", result,file_array[z].file_size );
                
			}
            else if(i==20)
            {
               // if((strcmp(result,".")==0)||(strcmp(result,"..")))
              //  {
              //      z--;
             //   }
                file_array[z].file_inner_path = (char *)malloc(FILENAME_LEN *sizeof(char));
				init_string(file_array[z].file_inner_path, FILENAME_LEN);
                strcpy(file_array[z].file_inner_path, result);
                file_array[z].full_path = (char *)malloc(PATH_LEN*sizeof(char));
				init_string(file_array[z].full_path, PATH_LEN);
                sprintf(file_array[z].full_path, "%s/%s",src_url,file_array[z].file_inner_path);
				file_array[z].dest_path = (char *)malloc(PATH_LEN*sizeof(char));
                init_string(file_array[z].dest_path, PATH_LEN);
                
                sprintf(file_array[z].dest_path, "%s/%s",dst_url,file_array[z].file_inner_path);
			}
				i++;
				result = strtok(NULL,"=; \t\n\r");
        }
        
    }
    
    *no_of_files = count_string;
    
    globus_ftp_client_handle_destroy(&handle);
    globus_ftp_client_handleattr_destroy(&handle_attr);
    globus_ftp_client_operationattr_destroy(&attr);
    
	reset_charbuffer();
    return file_array;
    
}

/*indicates that the concurrent transfer is complete */
static
void
done2_cb(
		 void *                                  user_arg,
		 globus_ftp_client_handle_t *            handle,
		 globus_object_t *                       err)
{
    char * tmpstr;
    if(err) tmpstr = " an";
    else    tmpstr = "out";
    
    if(err)
    { 
        printf("done transfer with%s error: %s\n", tmpstr, globus_object_printable_to_string(err)); 
        //error++; 
    }
    else fprintf(stdout,"no problem in transfer\n");
    globus_mutex_lock(&lock);
    //done = GLOBUS_TRUE;
    handle_count--;
    globus_cond_signal(&cond);
    globus_mutex_unlock(&lock);
    
}

static
void 
multichunk_done_cb(
		 void *                                  user_arg,
		 globus_ftp_client_handle_t *            handle,
		 globus_object_t *                       err)
{
    char * tmpstr;
    if(err) tmpstr = " an";
    else    tmpstr = "out";
    int handle_index = *((int *)user_arg);
    if(err)
    { 
        printf("done transfer with%s error: %s\n", tmpstr, globus_object_printable_to_string(err)); 
        //error++; 
    }
    else fprintf(stdout,"no problem in transfer\n");
    //printf("HANDLE_INDEX %d\n", handle_index);
	globus_mutex_lock(&lock);
    //done = GLOBUS_TRUE;
    handle_multichunk_count[handle_index]--;
    globus_cond_signal(&cond);
    globus_mutex_unlock(&lock);
    
}

/*to pipeline files in url pairs array*/
static
void 
pipeline_cb(
			globus_ftp_client_handle_t *                handle,
			char **                                     source_url,
			char **                                     dest_url,
			void *                                      user_arg)
{
    globus_l_gridftp_test_url_pairs_t *         url_pairs;
	// printf("PIPELINING\n");
    url_pairs = user_arg;
    if(url_pairs->index < url_pairs->count)
    {
        *source_url = url_pairs->source_array[url_pairs->index];
        *dest_url = url_pairs->dest_array[url_pairs->index];
        url_pairs->index++;
    }
    else
    {
        *source_url = NULL;
        *dest_url = NULL;
    }        
}
//time function 
void get_time_fun(double * tdouble, time_t * time_secs)
{
	struct timeval time;
	gettimeofday(&time, NULL);
	//double t1=time.tv_sec+(time.tv_usec/1000000.0);
	//printf("TIME %f\n",t1);
	char double_buf[50];
	sprintf(double_buf, "%ld.%06ld",(long int)time.tv_sec, (long int)time.tv_usec);
	*tdouble = atof(double_buf);
	*time_secs = time.tv_sec;
}


/*
 * Performs a transfer for the globus_file_t list with the given concurrency, pipelining and parallelism levels
 */
int perform_transfer(globus_file_t * file_array,int no_of_files,long chunk_size, int ppq, int p, int cc, double *thr)
{
    
    double start_with_msec;
	time_t start;
    get_time_fun(&start_with_msec, &start);
    
    
    globus_ftp_client_handle_t *                handle_list;
    globus_ftp_client_operationattr_t *         attr_list;
    globus_result_t                             result;
    globus_ftp_client_handleattr_t *            handle_attr_list;
    int i;
    globus_l_gridftp_test_url_pairs_t *         url_pairs_list;
    globus_ftp_control_parallelism_t		parallelism;
    
    globus_module_activate(GLOBUS_FTP_CLIENT_MODULE);
	
	int only_no_of_files = no_of_files;
	//SEPARATE DIRECTORIES FROM FILES AND CREATE THEM ON DESTINATION
	globus_ftp_client_handle_t			mkdir_handle;
    globus_ftp_client_operationattr_t		mkdir_attr;
    globus_ftp_client_handleattr_t		mkdir_handle_attr;
	//printf("NO OF FILES %d\n",no_of_files);
	for(i=0; i< no_of_files; i++)
	{
		if(strcmp(file_array[i].file_type, "file")!=0)
	    {
		
			if (strcmp(file_array[i].file_type, "dir")==0) {
				//make directory on destination
				
				
				globus_ftp_client_handleattr_init(&mkdir_handle_attr);
				globus_ftp_client_operationattr_init(&mkdir_attr);
				globus_mutex_init(&lock, GLOBUS_NULL);
				globus_cond_init(&cond, GLOBUS_NULL);
				globus_ftp_client_operationattr_set_type(&mkdir_attr,
														 GLOBUS_FTP_CONTROL_TYPE_ASCII);
				globus_ftp_client_handle_init(&mkdir_handle,  &mkdir_handle_attr);
				
				done = GLOBUS_FALSE;
				result = globus_ftp_client_mkdir(&mkdir_handle,
												 file_array[i].dest_path,
												 &mkdir_attr,
												 mkdir_done_cb,
												 0);
				if(result != GLOBUS_SUCCESS)
				{
					done = GLOBUS_TRUE;
				}
				
				globus_mutex_lock(&lock);
				while(!done)
				{
					globus_cond_wait(&cond, &lock);
				}
				globus_mutex_unlock(&lock);
				
				globus_ftp_client_handle_destroy(&mkdir_handle);
			}
	        only_no_of_files--; 	
		
		}
	
	}
	//printf("ONLY NO OF FILES %d\n",only_no_of_files);

	//TRANSFER FILES
	
	if(only_no_of_files < cc)
		cc = only_no_of_files;
    handle_count = cc;//initiate concurrency level
	
    //allocate memory for structures
    handle_list = globus_libc_malloc(cc * sizeof(globus_ftp_client_handle_t));
    attr_list = globus_libc_malloc(cc * sizeof(globus_ftp_client_operationattr_t));
    handle_attr_list = globus_libc_malloc(cc * sizeof(globus_ftp_client_handleattr_t));
    parallelism.mode = GLOBUS_FTP_CONTROL_PARALLELISM_FIXED;
    parallelism.fixed.size = p;
    //initiate attributes 
    for(i= 0 ; i<cc; i++)
    {
        globus_ftp_client_handleattr_init(&handle_attr_list[i]);
        globus_ftp_client_operationattr_init(&attr_list[i]);
		
    }
	globus_mutex_init(&lock, GLOBUS_NULL);
	globus_cond_init(&cond, GLOBUS_NULL);
    //allocate url pairs 
    url_pairs_list = globus_libc_malloc(cc * sizeof(globus_l_gridftp_test_url_pairs_t));
    int count = only_no_of_files / cc; 
    int remain = only_no_of_files % cc;
    //printf("count = %d remain= %d\n",count, remain);
    for(i = 0; i< remain ; i++)
    {    
		url_pairs_list[i].count = count+1;  
        //printf("1st url pairs list %d %d\n", i,url_pairs_list[i].count);
    }
    for(i=0; i< cc-remain; i++)
    {    
		url_pairs_list[i+remain].count = count;
        //printf("2nd url pairs list %d %d\n", i+remain,url_pairs_list[i+remain].count);
    }
    for(i=0; i<cc; i++)
    {   url_pairs_list[i].source_array = globus_malloc(url_pairs_list[i].count * sizeof(char *));
        url_pairs_list[i].dest_array = globus_malloc(url_pairs_list[i].count * sizeof(char *));
        url_pairs_list[i].index = 1;
    }
    
    //CONSTRUCT THE PIPELINING LISTS
    int j = 0;
    int counter[cc];
    for(i=0; i<cc;i++)
        counter[i]=0;
    i = 0;
    
    while(i<no_of_files)
    {   //printf("I=%d\n", i);
        for(j=0; j<cc &&i<no_of_files;)
        {   //printf("J= %d ",j);
            
			if(strcmp(file_array[i].file_type,"file")==0)
			{
            url_pairs_list[j].source_array[counter[j]] = globus_common_create_string("%s",file_array[i].full_path);
            //printf("SRC%s\n",  url_pairs_list[j].source_array[counter[j]]);
            url_pairs_list[j].dest_array[counter[j]++] = globus_common_create_string("%s",file_array[i].dest_path);
                j++;
            //url_pairs_list[j].dest_array[counter[j]++] = globus_common_create_string("%s",file_array[i].dest_path);
            //printf("DEST%s\n",  url_pairs_list[j].dest_array[counter[j]-1]);
            }
            i++;
            
        }
    }
    //INITIATE PIPELINE ATTR AND HANDLES 
    
    for(i=0; i< cc; i++)
    {  printf("INITIATING handles for pipelined concurrent transfer \n");
        globus_ftp_client_handleattr_set_pipeline(&handle_attr_list[i], ppq, pipeline_cb, &url_pairs_list[i]);
        globus_ftp_client_handle_init(&handle_list[i], &handle_attr_list[i]);
        globus_ftp_client_operationattr_set_mode(&attr_list[i], GLOBUS_FTP_CONTROL_MODE_EXTENDED_BLOCK);
        if(p>1)
            globus_ftp_client_operationattr_set_parallelism(&attr_list[i],
                                                            &parallelism);
        
    }
   
    //do the transfers 
    for(i=0; i< cc; i++)
    {   
        
        
        printf("Doing the transfer\n");
        result = globus_ftp_client_third_party_transfer(
                                                        &handle_list[i],
                                                        url_pairs_list[i].source_array[0],
                                                        &attr_list[i],
                                                        url_pairs_list[i].dest_array[0],
                                                        &attr_list[i],
                                                        GLOBUS_NULL,
                                                        done2_cb,
                                                        0);
        
        
        
    }
	
    globus_mutex_lock(&lock);
    while(handle_count>0)
    {
        globus_cond_wait(&cond, &lock);
    }
    globus_mutex_unlock(&lock);
    
   
    
    for(j=0; j<cc;j++)
    {
        for(i = 0; i < url_pairs_list[j].count; i++)
        {
            globus_free(url_pairs_list[j].source_array[i]);
            globus_free(url_pairs_list[j].dest_array[i]);
        }
        globus_free(url_pairs_list[j].source_array);
        globus_free(url_pairs_list[j].dest_array);
        
        globus_ftp_client_handle_destroy(&handle_list[j]);
        
        globus_ftp_client_handleattr_destroy(&handle_attr_list[j]);
        globus_ftp_client_operationattr_destroy(&attr_list[j]);
    }
    
    globus_mutex_destroy(&lock);
    globus_cond_destroy(&cond);
    
    globus_module_deactivate_all();
    
    
    double end_with_msec;
	time_t end;
	get_time_fun(&end_with_msec, &end);
    
    *thr = (chunk_size / (end_with_msec-start_with_msec))*8/1024.0/1024.0;
   // fprintf(ptr,"%ld\t%f\t%d\t%d\t%d\n",chunk_size,*thr,ppq,p,cc);
    
   // printf("CHUNKSIZE %ld ELAPSED SECS:%f\n",chunk_size, end_with_msec - start_with_msec );
   // fclose(ptr);
	 
	 
    if(error)
    {
        return 0;
    }
    
    return error;
    //return 0;;
    
}



/*
* Perform transfers with multiple globus_file_t lists at the same time
*/



int perform_multichunk_transfer(globus_file_t ** chunk_array, 
								int no_of_replicas, 
								int total_no_files,
								int * no_files_per_chunk, 
								long total_size, 
								int ppq, 
								int p, 
								int cc_per_chunk, 
								double *thr)
{
    
    double start_with_msec;
	time_t start;
    get_time_fun(&start_with_msec, &start);
    
    
    globus_ftp_client_handle_t **                handle_list;
    globus_ftp_client_operationattr_t **         attr_list;
    globus_result_t                             result;
    globus_ftp_client_handleattr_t **            handle_attr_list;
    int i,j,k;
    globus_l_gridftp_test_url_pairs_t **         url_pairs_list;
    globus_ftp_control_parallelism_t		parallelism;
    
    globus_module_activate(GLOBUS_FTP_CLIENT_MODULE);
	
	
	//SEPARATE DIRECTORIES FROM FILES AND CREATE THEM ON DESTINATION
	globus_ftp_client_handle_t			mkdir_handle;
    globus_ftp_client_operationattr_t		mkdir_attr;
    globus_ftp_client_handleattr_t		mkdir_handle_attr;
	//printf("NO OF FILES %d\n",no_of_files);	
	int *only_no_of_files = malloc(no_of_replicas*sizeof(int));
	for (j=0; j<no_of_replicas; j++)
	{   
		
		only_no_of_files[j] = no_files_per_chunk[j];
		for(i=0; i< no_files_per_chunk[j]; i++)
		{
			if(strcmp(chunk_array[j][i].file_type, "file")!=0)
			{
			
				if (strcmp(chunk_array[j][i].file_type, "dir")==0) {
				//make directory on destination
				
				
					globus_ftp_client_handleattr_init(&mkdir_handle_attr);
					globus_ftp_client_operationattr_init(&mkdir_attr);
					globus_mutex_init(&lock, GLOBUS_NULL);
					globus_cond_init(&cond, GLOBUS_NULL);
					globus_ftp_client_operationattr_set_type(&mkdir_attr,
														 GLOBUS_FTP_CONTROL_TYPE_ASCII);
					globus_ftp_client_handle_init(&mkdir_handle,  &mkdir_handle_attr);
				
					done = GLOBUS_FALSE;
					result = globus_ftp_client_mkdir(&mkdir_handle,
												 chunk_array[j][i].dest_path,
												 &mkdir_attr,
												 mkdir_done_cb,
												 0);
					if(result != GLOBUS_SUCCESS)
					{
						done = GLOBUS_TRUE;
					}
				
					globus_mutex_lock(&lock);
					while(!done)
					{
						globus_cond_wait(&cond, &lock);
					}
					globus_mutex_unlock(&lock);
				
					globus_ftp_client_handle_destroy(&mkdir_handle);
				}
				only_no_of_files[j]--; 	
			
			}
		
		}
	
	}
	
	
	
	//TRANSFER FILES
	int *cc = malloc(no_of_replicas * sizeof(int));
	handle_multichunk_count = malloc(no_of_replicas * sizeof(int));
	
	for(j = 0; j< no_of_replicas; j++)
	{	
		
		//printf("ONLY NO OF FILES %d\n",only_no_of_files[j]);
		cc[j] = cc_per_chunk;
		if(only_no_of_files[j] < cc_per_chunk)
			cc[j] = only_no_of_files[j];
        
		handle_multichunk_count[j] = cc[j];//initiate concurrency level everytime not here
	}
		
    //allocate memory for structures
		handle_list = (globus_ftp_client_handle_t **)malloc(no_of_replicas * sizeof(globus_ftp_client_handle_t *));
		attr_list = (globus_ftp_client_operationattr_t **)malloc(no_of_replicas * sizeof(globus_ftp_client_operationattr_t *));
		handle_attr_list = (globus_ftp_client_handleattr_t **)malloc(no_of_replicas * sizeof(globus_ftp_client_handleattr_t *));
	
	for(j = 0; j< no_of_replicas;j++ )	
		{
			handle_list[j] = globus_libc_malloc(cc[j]*sizeof(globus_ftp_client_handle_t));
			attr_list[j] = globus_libc_malloc(cc[j] * sizeof(globus_ftp_client_operationattr_t));
			handle_attr_list[j] = globus_libc_malloc(cc[j] * sizeof(globus_ftp_client_handleattr_t));
		
		}
    //COULD BE SET SEPARATELY TOO
    parallelism.mode = GLOBUS_FTP_CONTROL_PARALLELISM_FIXED;
    parallelism.fixed.size = p;
    //initiate attributes 
    
	for(j = 0; j<no_of_replicas; j++)
		for(i= 0 ; i<cc[j]; i++)
		{
			globus_ftp_client_handleattr_init(&handle_attr_list[j][i]);
			globus_ftp_client_operationattr_init(&attr_list[j][i]);
		
		}
	globus_mutex_init(&lock, GLOBUS_NULL);
	globus_cond_init(&cond, GLOBUS_NULL);
    //allocate url pairs
	
	url_pairs_list = (globus_l_gridftp_test_url_pairs_t **)malloc(no_of_replicas * sizeof(globus_l_gridftp_test_url_pairs_t *));	
	

	for(j = 0; j< no_of_replicas; j++)
	{
		

		url_pairs_list[j] =(globus_l_gridftp_test_url_pairs_t *) globus_libc_malloc(cc[j] * sizeof(globus_l_gridftp_test_url_pairs_t));
		

		int count = only_no_of_files[j] / cc[j];         
		int remain = only_no_of_files[j] % cc[j];
		//printf("count = %d remain= %d\n",count, remain);
		
		for(i = 0; i< remain ; i++)
		{    
			url_pairs_list[j][i].count = count+1;  
			//printf("1st url pairs list %d %d\n", i,url_pairs_list[i].count);
		}
		for(i=0; i< cc[j]-remain; i++)
		{    
			url_pairs_list[j][i+remain].count = count;
			//printf("2nd url pairs list %d %d\n", i+remain,url_pairs_list[i+remain].count);
		}
		for(i=0; i<cc[j]; i++)
		{   url_pairs_list[j][i].source_array = globus_malloc(url_pairs_list[j][i].count * sizeof(char *));
			url_pairs_list[j][i].dest_array = globus_malloc(url_pairs_list[j][i].count * sizeof(char *));
			url_pairs_list[j][i].index = 1;
		}
		
	} 
		
    
    
	
    
    
    //CONSTRUCT THE PIPELINING LISTS
		int * counter[no_of_replicas];
		for(j = 0; j < no_of_replicas; j++)
		{
		    counter[j] = malloc(cc[j] *sizeof(int));
		    for(i=0; i<cc[j];i++)
				counter[j][i]=0;
			
			k = 0;
			i = 0;
			while(i<no_files_per_chunk[j])
			{   //printf("I=%d\n", i);
				for(k=0; k<cc[j] &&i<no_files_per_chunk[j]; )
				{   //printf("J= %d ",j);
					
					if(strcmp(chunk_array[j][i].file_type,"file")==0)
					{
						url_pairs_list[j][k].source_array[counter[j][k]] = globus_common_create_string("%s",chunk_array[j][i].full_path);
						//printf("%d:%d:SRC%s\t", j,k, url_pairs_list[j][k].source_array[counter[j][k]]);
						url_pairs_list[j][k].dest_array[counter[j][k]++] = globus_common_create_string("%s",chunk_array[j][i].dest_path);
						//url_pairs_list[j].dest_array[counter[j]++] = globus_common_create_string("%s",file_array[i].dest_path);
						//printf("%d:%d:DEST%s\n",j,k,  url_pairs_list[j][k].dest_array[counter[j][k]-1]);
						k++;
					}
					i++;
					
				}
			}
			
		
		}

    
    
    //INITIATE PIPELINE ATTR AND HANDLES 
    
	for(j=0;j<no_of_replicas;j++)	
    {
		for(i=0; i< cc[j]; i++)
		{
            printf("INITIATING handles for pipelined concurrent transfer \n");
			globus_ftp_client_handleattr_set_pipeline(&handle_attr_list[j][i], ppq, pipeline_cb, &url_pairs_list[j][i]);
			globus_ftp_client_handle_init(&handle_list[j][i], &handle_attr_list[j][i]);
			globus_ftp_client_operationattr_set_mode(&attr_list[j][i], GLOBUS_FTP_CONTROL_MODE_EXTENDED_BLOCK);
			if(p>1)
				globus_ftp_client_operationattr_set_parallelism(&attr_list[j][i],
                                                            &parallelism);
        
		}
	
		//do the transfers 
		for(i=0; i< cc[j]; i++)
		{   
        
        
			printf("Doing the transfer\n");
			result = globus_ftp_client_third_party_transfer(
                                                        &handle_list[j][i],
                                                        url_pairs_list[j][i].source_array[0],
                                                        &attr_list[j][i],
                                                        url_pairs_list[j][i].dest_array[0],
                                                        &attr_list[j][i],
                                                        GLOBUS_NULL,
                                                        multichunk_done_cb,
                                                        &j);
		}
	
	}	
	
    
	
	for(j=0; j<no_of_replicas; j++)
	{
		globus_mutex_lock(&lock);
		while(handle_multichunk_count[j]>0)
		{
			globus_cond_wait(&cond, &lock);
		}
		globus_mutex_unlock(&lock);
		//printf("unlocking!\n");
    }
	
    
	for(k=0;k<no_of_replicas;k++)
	{	
		for(j=0; j<cc[k];j++)
		{
			for(i = 0; i < url_pairs_list[k][j].count; i++)
			{
				globus_free(url_pairs_list[k][j].source_array[i]);
				globus_free(url_pairs_list[k][j].dest_array[i]);
			}
			globus_free(url_pairs_list[k][j].source_array);
			globus_free(url_pairs_list[k][j].dest_array);
        
			globus_ftp_client_handle_destroy(&handle_list[k][j]);
        
			globus_ftp_client_handleattr_destroy(&handle_attr_list[k][j]);
			globus_ftp_client_operationattr_destroy(&attr_list[k][j]);
        }
    }
    globus_mutex_destroy(&lock);
    globus_cond_destroy(&cond);
    
    globus_module_deactivate_all();
    
    
    double end_with_msec;
	time_t end;
	get_time_fun(&end_with_msec, &end);
    *thr = (total_size/(end_with_msec-start_with_msec)*8/1024.0/1024.0);
		
    if(error)
    {
        return 0;
    }
    
    return error;
    //return 0;;
    
	
}


