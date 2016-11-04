/*
 *  concurrency_basic_alg.c
 *
 *
 *  Created by esma yildirim on 11/02/16.
 *  Copyright 2016 __Rutgers University__. All rights reserved.
 *
 */

#include "general_utility.h"

int global_cc = 1;
double prev_throughput = -1;
void recursive_optimal_pp(globus_file_t * file_list, int no_of_files, long min_chunk_size, int parent_pp, int max_pp, long BDP);
void recursive_optimal_pp_with_proportional_cc(globus_file_t * file_list, int no_of_files, long min_chunk_size, int parent_pp, int max_pp, long BDP);

/*
 globus-url-copy -pp <src_path> <dest_path>
 */
int baseline_algorithm(char * src, char *dst )
{
    globus_file_t * file_array;
    
    int total_no_files;
    double thr = 0;
    
    file_array = list_files(src, dst, file_array, &total_no_files);
    perform_transfer(file_array,total_no_files,getsize(file_array,total_no_files),20,1,1,&thr);
    printf("THR %f\n",thr);
    free(file_array);
    return 0;
}
/*
 *Adaptive concurreny changing algorithm: increase/decrease concurrency based on
 *the ratio of current throughput to the previous one
 */
int proportional_cc(char *src,char *dst, int no_of_files_in_batch)
{
    globus_file_t * file_list;
    int total;
    file_list = list_files(src, dst, file_list, &total);
    
    
    
    if(no_of_files_in_batch > total) {  printf("batch no must be less\n"); exit(1);}
    
    int current_count = 0;
    int number_of_batches = total/no_of_files_in_batch;
    int remainder = total % no_of_files_in_batch;
    
    globus_file_t * new_list;
    int iteration=0;
    int cc = 1;
    double prev_thr, thr;
    
    long long chunk_size = 0;
    double proportion;
    // do the first transfer for baseline performance
    new_list = (globus_file_t *) malloc(no_of_files_in_batch * sizeof(globus_file_t));
    
    for(int i = 0; i < no_of_files_in_batch; i++, current_count++)
    {
        new_list[i] = file_list[current_count];
        chunk_size += new_list[i].file_size;
    }
    perform_transfer(new_list, no_of_files_in_batch,chunk_size, 0, 0, cc, &thr);
    free(new_list);
    cc *=2;
    prev_thr = thr;
    iteration++;
    
    while(iteration < number_of_batches)
    {
        if(iteration == number_of_batches -1)
            no_of_files_in_batch += remainder;
        new_list = (globus_file_t *) malloc(no_of_files_in_batch * sizeof(globus_file_t));
        chunk_size = 0;
        for(int i = 0; i < no_of_files_in_batch; i++, current_count++)
        {
            new_list[i] = file_list[current_count];
            chunk_size += new_list[i].file_size;
        }
        perform_transfer(new_list, no_of_files_in_batch,chunk_size, 0, 0, cc, &thr);
        free(new_list);
        
        proportion = thr / prev_thr;
        printf("cc = %d thr = %f proportion = %f\n",cc,thr, proportion);
        
        cc = ceil(cc * proportion);
        prev_thr = thr;
        iteration++;
        
    }
    
    return 0;
}

/*
 *Adaptive concurreny changing algorithm: increase/decrease concurrency exponentially
 */
int adaptive_concurrency_algorithm(char * src, char *dst,int chunk_size,int pp,int p )
{
    globus_file_t * file_array;
    
    int total_no_files;
    double thr = 0;
    int cc = 1;
    double prev_thr = 0;
    int transferred_files = 0;
    file_array = list_files(src, dst, file_array, &total_no_files);
    int temp_total_no_files = total_no_files;
    
    while(transferred_files < temp_total_no_files)
    {
        globus_file_t * new_chunk;
        if((temp_total_no_files - transferred_files)<= chunk_size)
            chunk_size = temp_total_no_files - transferred_files;
        new_chunk = split_list(file_array,&total_no_files,new_chunk,&chunk_size);
        perform_transfer(new_chunk,chunk_size,getsize(new_chunk, chunk_size),pp,p,cc,&thr);
        if(thr < 0.9*prev_thr && cc>=2)
            cc=cc/2;
        else if(thr >= 1.1*prev_thr && cc < 64) cc *=2;
        prev_thr = thr;
        transferred_files += chunk_size;
        free(new_chunk);
    }
    free(file_array);
    return 0;
}

/*
 * Transfers the dataset from multiple replica locations concurrently
 *
 */
int concurrent_replica_algorithm(char **replica_src_list,int no_of_replicas, char *dst, int cc_per_replica, int pp, int p)
{
    
    globus_file_t * main_file_array;
    globus_file_t ** replica_arrays;
    int total_no_files;
    int i,j;
    long total_size;
    replica_arrays = (globus_file_t **)malloc(no_of_replicas * sizeof(globus_file_t *));//not so sure
    main_file_array = list_files(replica_src_list[0], dst, main_file_array, &total_no_files);
    total_size = getsize(main_file_array,total_no_files);
    int chunk_size = total_no_files / no_of_replicas;
    int remainder = total_no_files % no_of_replicas;
    int * no_files_per_chunk = malloc(no_of_replicas*sizeof(int));
    
    for (i=0; i< no_of_replicas; i++) {
        if(i == no_of_replicas-1) chunk_size +=remainder;
        replica_arrays[i] = split_list(main_file_array, &total_no_files,replica_arrays[i],&chunk_size);
        no_files_per_chunk[i] = chunk_size;
        if(i !=0)
        {
            //CHANGE SRC
            for (j=0; j<chunk_size; j++) {
                init_string(replica_arrays[i][j].full_path, PATH_LEN);
                sprintf(replica_arrays[i][j].full_path,"%s/%s",replica_src_list[i],replica_arrays[i][j].file_inner_path);
            }
            
        }
        
    }
    double thr;
    perform_multichunk_transfer(replica_arrays,
                                no_of_replicas,
                                total_no_files,
                                no_files_per_chunk,
                                total_size,
                                pp,
                                p,
                                cc_per_replica,
                                &thr);
    
    return 0;
    
}

/*
 * Conducts independent src-dest transfers concurrently
 *
 */
int multinode_concurrency_algorithm(char **src_list,char **dst_list,int no_of_nodes, int cc_per_node, int pp, int p)
{
    globus_file_t **node_arrays;
    int *no_files_per_chunk;
    int total_no_files = 0;
    long total_size = 0;
    int i;
    double thr;
    node_arrays = (globus_file_t **)malloc(no_of_nodes * sizeof(globus_file_t *));//not so sure
    no_files_per_chunk = (int *)malloc(no_of_nodes * sizeof(int));
    for (i=0; i<no_of_nodes; i++) {
        
        node_arrays[i] = list_files(src_list[i], dst_list[i], node_arrays[i], &no_files_per_chunk[i]);
        total_no_files += no_files_per_chunk[i];
        total_size += getsize(node_arrays[i],no_files_per_chunk[i]);
        //printf("NO OF FILES in main %d\n",no_files_per_chunk[i]);
    }
    perform_multichunk_transfer(node_arrays,
                                no_of_nodes,
                                total_no_files,
                                no_files_per_chunk,
                                total_size,
                                pp,
                                p,
                                cc_per_node,
                                &thr);
    
    printf("THROUGHPUT %f Mbps\n", thr);
    return 0;
    
}
// necessary structure for constructing an overlay data transfer graph
typedef struct _transfer_node_t{
    
    int id;
    int has_parents;
    int has_children;
    int no_of_parents;
    int no_of_children;
    int *parents;
    int *children;
    globus_file_t *list;
    int list_size;
    int level;
    
    
}transfer_node_t;

int overlay_concurrency_algorithm(int no_of_transfers)
{
    char *src_path0 = "ftp://localhost:5000/Users/esmayildirim/files";
    char *dst_path0 = "ftp://localhost:5000/Users/esmayildirim/files2";
    char *dst_path1 = "ftp://localhost:5000/Users/esmayildirim/files3";
    char *dst_path2 = "ftp://localhost:5000/Users/esmayildirim/files4";
    
    
    globus_file_t *initial_list;
    int total_no_files;
    initial_list = list_files(src_path0,dst_path0,initial_list,&total_no_files);
    int _total_no_files = total_no_files;
    globus_file_t ** list_array = (globus_file_t **)malloc(no_of_transfers *sizeof(globus_file_t *));
    transfer_node_t * transfers = (transfer_node_t *)malloc(no_of_transfers*sizeof(transfer_node_t));
    int size1,size2;
    size1 = total_no_files/2;
    size2 = total_no_files - size1;
    //transfer 0 - lıstfiles and splıt into two, take the first list
    list_array[0] = split_list(initial_list,&total_no_files, list_array[0],&size1);
    //enqueue(list_array, &no_transfers_in_the_list, list_array[0]);
    transfers[0].list = list_array[0];
    transfers[0].list_size = size1;
    
    //transfer 1 -take the second list- change the destination path
    list_array[1] = split_list(initial_list, &total_no_files, list_array[1],&size2);
    //enqueue(list_array)
    int i ;
    for (i=0; i<size2; i++) {
        init_string(list_array[1][i].dest_path, PATH_LEN);
        sprintf(list_array[1][i].dest_path,"%s/%s",dst_path1,list_array[1][i].file_inner_path);
    }
    transfers[1].list = list_array[1];
    transfers[1].list_size = size2;
    
    //transfer 2 - create a new list - copy files from transfer 0 and change dest as src
    
    list_array[2] = (globus_file_t *)malloc(transfers[0].list_size*sizeof(globus_file_t));
    printf("TRANSFER 0.listsize %d\n",transfers[0].list_size );
    for(i= 0; i <transfers[0].list_size; i++)
    {
        list_array[2][i].file_inner_path = (char *)malloc(sizeof(char)*FILENAME_LEN);
        init_string(list_array[2][i].file_inner_path, FILENAME_LEN);
        strncpy(list_array[2][i].file_inner_path, list_array[0][i].file_inner_path,FILENAME_LEN-1);
        //printf("%s\t",list_array[2][i].file_inner_path);
        list_array[2][i].full_path = (char *)malloc(sizeof(char)*PATH_LEN);
        init_string(list_array[2][i].full_path, PATH_LEN);
        strncpy(list_array[2][i].full_path, list_array[0][i].dest_path,PATH_LEN-1);
        //printf("%s\t",list_array[2][i].full_path);
        
        list_array[2][i].dest_path = (char *)malloc(sizeof(char)*PATH_LEN);
        init_string(list_array[2][i].dest_path, PATH_LEN);
        sprintf(list_array[2][i].dest_path,"%s/%s",dst_path2,list_array[0][i].file_inner_path);
        //printf("%s\n",list_array[2][i].dest_path);
        
        list_array[2][i].file_size = list_array[0][i].file_size;
        strncpy(list_array[2][i].file_type, list_array[0][i].file_type,4);
        
    }
    
    transfers[2].list = list_array[2];
    transfers[2].list_size = transfers[0].list_size;
    
    //transfer 3 - create a new list - copy files from transfer 1 and change dest as src
    list_array[3] = (globus_file_t *)malloc(transfers[1].list_size*sizeof(globus_file_t));
    printf("TRANSFER 0.listsize %d\n",transfers[1].list_size );
    for(i= 0; i <transfers[1].list_size; i++)
    {
        list_array[3][i].file_inner_path = (char *)malloc(sizeof(char)*FILENAME_LEN);
        init_string(list_array[3][i].file_inner_path, FILENAME_LEN);
        strncpy(list_array[3][i].file_inner_path, list_array[1][i].file_inner_path,FILENAME_LEN-1);
        
        list_array[3][i].full_path = (char *)malloc(sizeof(char)*PATH_LEN);
        init_string(list_array[3][i].full_path, PATH_LEN);
        strncpy(list_array[3][i].full_path, list_array[1][i].dest_path,PATH_LEN-1);
        
        list_array[3][i].dest_path = (char *)malloc(sizeof(char)*PATH_LEN);
        init_string(list_array[3][i].dest_path, PATH_LEN);
        sprintf(list_array[3][i].dest_path,"%s/%s",dst_path2,list_array[1][i].file_inner_path);
        list_array[3][i].file_size = list_array[1][i].file_size;
        strncpy(list_array[3][i].file_type, list_array[1][i].file_type,4);
        
    }
    
    transfers[3].list = list_array[3];
    transfers[3].list_size = transfers[1].list_size;
    
    //transfer structure almost ready
    //prepare neigbouring relationships
    
    transfers[0].has_parents = 0;
    transfers[0].has_children = 1;
    transfers[0].children = (int *)malloc(sizeof(int)*1);
    transfers[0].children[0] = 2;
    transfers[0].no_of_parents = 0;
    transfers[0].no_of_children = 1;
    transfers[0].id = 0;
    transfers[0].level = 0;
    
    transfers[1].has_parents = 0;
    transfers[1].has_children = 1;
    transfers[1].children = (int *)malloc(sizeof(int)*1);
    transfers[1].children[0] = 3;
    transfers[1].no_of_parents = 0;
    transfers[1].no_of_children = 1;
    transfers[1].id = 1;
    transfers[1].level = 0;
    
    transfers[2].has_parents = 1;
    transfers[2].has_children = 0;
    transfers[2].parents = (int *)malloc(sizeof(int)*1);
    transfers[2].parents[0] = 0;
    transfers[2].no_of_parents = 1;
    transfers[2].no_of_children = 0;
    transfers[2].id = 2;
    transfers[2].level = 1;
    
    transfers[3].has_parents = 1;
    transfers[3].has_children = 0;
    transfers[3].parents = (int *)malloc(sizeof(int)*1);
    transfers[3].parents[0] = 1;
    transfers[3].no_of_parents = 1;
    transfers[3].no_of_children = 0;
    transfers[3].id = 3;
    transfers[3].level = 1;
    
    int j,k;
    int level=0;
    i=0;
    double thr;
    int no_of_transfers_in_a_list=0;
    globus_file_t **lists = malloc(no_of_transfers*sizeof(globus_file_t*));
    int *no_of_files_per_chunk = malloc(no_of_transfers*sizeof(int));
    long total_chunk_size = 0;
    _total_no_files = 0;
    printf("NO_OF_TRANSFERS %d\n",no_of_transfers);
    for(j = 0 ; j< no_of_transfers;)
    {
        while(transfers[j].level<=level && j<no_of_transfers )
        {
            
            lists[i] = transfers[j].list;
            
            
            no_of_transfers_in_a_list++;
            no_of_files_per_chunk[i]=transfers[j].list_size;
            _total_no_files +=no_of_files_per_chunk[i];
            
            
            total_chunk_size += getsize(transfers[j].list,transfers[j].list_size);
            
            
            i++;
            j++;
            
        }
        perform_multichunk_transfer(lists,
                                    no_of_transfers_in_a_list,
                                    _total_no_files,
                                    no_of_files_per_chunk,
                                    total_chunk_size,
                                    20,
                                    1,
                                    2,
                                    &thr);
        
        i=0;
        no_of_transfers_in_a_list = 0;
        _total_no_files=0;
        total_chunk_size = 0;
        level++;
        
        
        
    }
    
    
    return 0;
}

/*
 * set different pipelining levels based on the file size distribution in the dataset
 */
void optimal_pp_algorithm(char *src, char *dst, long BDP, int max_pp, long min_chunk_size)
{
    globus_file_t *file_list;
    int total_no_files;
    file_list = list_files(src, dst, file_list, &total_no_files);
    quick_sort_by_size(file_list,0,total_no_files-1);
    // printf("file_list size %d",total_no_files);
    //recursive_optimal_pp(file_list, total_no_files, min_chunk_size,0,max_pp,BDP);
    recursive_optimal_pp_with_proportional_cc(file_list, total_no_files, min_chunk_size,0,max_pp,BDP);
    
}
// This algorithm sets the optimal pp parameter for each chunk
void recursive_optimal_pp(globus_file_t * file_list, int no_of_files, long min_chunk_size, int parent_pp, int max_pp, long BDP)
{
    
    
    int i;
    double thr;
    long mean_file_size = 0;
    int current_optimal_pp;
    int temp_no_of_files = no_of_files;
    long chunk_size = getsize(file_list, no_of_files);
    for (i=0; i<no_of_files; i++) {
        mean_file_size +=file_list[i].file_size;
    }
    mean_file_size /=no_of_files;
    current_optimal_pp = (BDP/mean_file_size)+2;
    printf("mean_file_size %ld, current_optimal_pp %d\n",mean_file_size,current_optimal_pp);
    int size1=0,size2=0;
    for (i=0; i<no_of_files; i++) {
        if(file_list[i].file_size < mean_file_size)
            size1 ++;
    }
    size2 = no_of_files-size1;
    //printf("Size1 %d Size2 %d\n",size1,size2);
    
    globus_file_t *new_chunk1, *new_chunk2;
    if(size1>0)
        new_chunk1 = split_list(file_list,&no_of_files,new_chunk1,&size1);
    if(size2>0)
        new_chunk2 = split_list(file_list,&no_of_files,new_chunk2,&size2);
    if(chunk_size> 2*min_chunk_size &&current_optimal_pp !=1&&current_optimal_pp !=parent_pp && current_optimal_pp < max_pp )
    {
        if(size1>0)
            recursive_optimal_pp(new_chunk1,size1,min_chunk_size,current_optimal_pp,max_pp,BDP);
        if(size2>0)
            recursive_optimal_pp(new_chunk2,size2,min_chunk_size,current_optimal_pp,max_pp,BDP);
        
    }
    else {
        if(parent_pp>0)
            current_optimal_pp = parent_pp;
        else {
            if (current_optimal_pp>max_pp) {
                current_optimal_pp = max_pp;
            }
        }
        
        perform_transfer(file_list,temp_no_of_files,chunk_size,current_optimal_pp,1,1,&thr);
        printf("Throughput= %f\n",thr);
    }
    
}

void set_global_cc(double throughput)
{
    if(prev_throughput == -1)
        global_cc *= 2 ;
    else
        global_cc = ceil(global_cc * throughput/prev_throughput);
    prev_throughput = throughput;
}
// This algorithm sets the optimal pp parameter for each chunk and adaptively changes cc value based on the proportion thr/prev_thr value
void recursive_optimal_pp_with_proportional_cc(globus_file_t * file_list, int no_of_files, long min_chunk_size, int parent_pp, int max_pp, long BDP)
{
    
    
    int i;
    double thr;
    long mean_file_size = 0;
    int current_optimal_pp;
    int temp_no_of_files = no_of_files;
    long chunk_size = getsize(file_list, no_of_files);
    for (i=0; i<no_of_files; i++) {
        mean_file_size +=file_list[i].file_size;
    }
    mean_file_size /=no_of_files;
    current_optimal_pp = (BDP/mean_file_size)+2;
    printf("mean_file_size %ld, current_optimal_pp %d\n",mean_file_size,current_optimal_pp);
    int size1=0,size2=0;
    for (i=0; i<no_of_files; i++) {
        if(file_list[i].file_size < mean_file_size)
            size1 ++;
    }
    size2 = no_of_files-size1;
    //printf("Size1 %d Size2 %d\n",size1,size2);
    
    globus_file_t *new_chunk1, *new_chunk2;
    if(size1>0)
        new_chunk1 = split_list(file_list,&no_of_files,new_chunk1,&size1);
    if(size2>0)
        new_chunk2 = split_list(file_list,&no_of_files,new_chunk2,&size2);
    if(chunk_size> 2*min_chunk_size &&current_optimal_pp !=1&&current_optimal_pp !=parent_pp && current_optimal_pp < max_pp )
    {
        if(size1>0)
            recursive_optimal_pp_with_proportional_cc(new_chunk1,size1,min_chunk_size,current_optimal_pp,max_pp,BDP);
        if(size2>0)
            recursive_optimal_pp_with_proportional_cc(new_chunk2,size2,min_chunk_size,current_optimal_pp,max_pp,BDP);
        
    }
    else {
        if(parent_pp>0)
            current_optimal_pp = parent_pp;
        else {
            if (current_optimal_pp>max_pp) {
                current_optimal_pp = max_pp;
            }
        }
        perform_transfer(file_list,temp_no_of_files,chunk_size,current_optimal_pp,1,global_cc,&thr);
        printf("Throughput= %f CC=%d\n",thr, global_cc);
        set_global_cc(thr);
        
        //printf("Throughput= %f\n",thr);
    }
    
    
    
    
}
int main(int argc, char **argv)
{
    
    char * src;
    char * dst;
    src = strdup(argv[1]); // source URL
    dst = strdup(argv[2]); // destination URL
    baseline_algorithm(src, dst );
    
    proportional_cc(src,dst, 4);
    //adaptive_concurrency_algorithm(src, dst, 500,4,2);
    
    char ** replica_list = (char **)malloc(3*sizeof(char *));
    replica_list[0] = strdup("ftp://localhost:5000/Users/esmayildirim/files");
    replica_list[1] = strdup("ftp://localhost:5000/Users/esmayildirim/files2");
    replica_list[2] = strdup("ftp://localhost:5000/Users/esmayildirim/files3");
    concurrent_replica_algorithm(replica_list,3, dst,5, 4,2);
    
    char ** src_list = (char **)malloc(2*sizeof(char *));
    char ** dst_list = (char **)malloc(2*sizeof(char *));
    src_list[0] = strdup("ftp://localhost:5000/Users/esmayildirim/files");
    src_list[1] = strdup("ftp://localhost:5000/Users/esmayildirim/files2");
    dst_list[0] = strdup("ftp://localhost:5000/Users/esmayildirim/files3");
    dst_list[1] = strdup("ftp://localhost:5000/Users/esmayildirim/files4");
    multinode_concurrency_algorithm(src_list,dst_list,2, 5, 10,  2);
    
    
    long BDP = 100000000;//bytes
    int max_pp = 32;
    int min_chunk_size = 50000000;//bytes
    optimal_pp_algorithm(src, dst, BDP, max_pp, min_chunk_size);
    
    //overlay algorithm
    overlay_concurrency_algorithm(4);
    
    return 0;
}

