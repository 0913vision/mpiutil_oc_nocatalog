#include <stdio.h>
#include <stdint.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>

#include "libcircle.h"
#include "dtcmp.h"
#include "mfu.h"

uint64_t num_tasks_per_ost[OST_NUMBER];

/****************************************
 * Functions to divide flist into linked list of file sections
 ***************************************/

/* given a file offset, the rank of the last process to hold an
 * extra chunk, and the number of chunks per rank, compute
 * and return the rank of the chunk at the given offset */
static int map_chunk_to_rank(uint64_t offset, uint64_t cutoff, uint64_t chunks_per_rank)
{
    /* total number of chunks held by ranks below cutoff */
    uint64_t cutoff_coverage = cutoff * (chunks_per_rank + 1);

    /* given an offset of a chunk, identify which rank will
     * be responsible */
    int rank;
    if (offset < cutoff_coverage) {
        rank = (int) (offset / (chunks_per_rank + 1));
    } else {
        rank = (int) (cutoff + (offset - cutoff_coverage) / chunks_per_rank);
    }

    return rank;
}


/* This is a long routine, but the idea is simple.  All tasks sum up
 * the number of file chunks they have, and those are then evenly
 * distributed amongst the processes.  */
mfu_file_chunk* mfu_file_chunk_list_alloc(mfu_flist list, uint64_t chunk_size)
{
    /* get our rank and number of ranks */
    int rank, ranks;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &ranks);
	
    mfu_file_chunk* head = NULL;
    mfu_file_chunk* tail = NULL;

    int rc[5];
    uint64_t cnt = 0, ssize, start, end, ost_idx, interval, file_size;
    int worker_number = ranks;
    int obj_idx = 0;
    struct llapi_layout *layout;


    /* total up number of file chunks for all files in our list */
    uint64_t count = 0;
    uint64_t idx;
    uint64_t size = mfu_flist_size(list);
    for (idx = 0; idx < size; idx++) {
        /* get type of item */
        mfu_filetype type = mfu_flist_file_get_type(list, idx);

        /* if we have a file, add up its chunks */
        if (type == MFU_TYPE_FILE) {
            /* get size of file */
            uint64_t file_size = mfu_flist_file_get_size(list, idx);

            /* compute number of chunks to copy for this file */
            uint64_t chunks = file_size / chunk_size;
            if (chunks * chunk_size < file_size || file_size == 0) {
                /* this accounts for the last chunk, which may be
 *                  * partial or it adds a chunk for 0-size files */
                chunks++;
            }

            /* include these chunks in our total */
            count += chunks;
        }
    }
	printf("rank %d chunks: %d\n", rank, count);

    /* compute total number of chunks across procs */
    uint64_t total;
    MPI_Allreduce(&count, &total, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);

	printf("total chunks: %d\n", total);

    /* get global offset of our first chunk */
    uint64_t offset;
    MPI_Exscan(&count, &offset, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
    if (rank == 0) {
        offset = 0;
    }

    /* if we have some chunks, figure out the number of ranks
 *      * we'll send to and the range of rank ids, set flags to 1 */

    /* allocate a linked list for each process we'll send to */
    mfu_file_chunk** heads = (mfu_file_chunk**) MFU_MALLOC((size_t)ranks * sizeof(mfu_file_chunk*));
    mfu_file_chunk** tails = (mfu_file_chunk**) MFU_MALLOC((size_t)ranks * sizeof(mfu_file_chunk*));
    uint64_t* counts  = (uint64_t*)   MFU_MALLOC((size_t)ranks * sizeof(uint64_t));
    uint64_t* bytes   = (uint64_t*)   MFU_MALLOC((size_t)ranks * sizeof(uint64_t));
    char** sendbufs   = (char**)      MFU_MALLOC((size_t)ranks * sizeof(char*));
	
    /* initialize values */
    for (int i = 0; i < ranks; i++) {
        heads[i]    = NULL;
        tails[i]    = NULL;
        counts[i]   = 0;
        bytes[i]    = 0;
        sendbufs[i] = NULL;
    }

    /* now iterate through files and build up list of chunks we'll
 *      * send to each task, as an optimization, we encode consecutive
 *           * chunks of the same file into a single unit */
 
   uint64_t current_offset = offset;
    for (idx = 0; idx < size; idx++) {
        /* get type of item */
        mfu_filetype type = mfu_flist_file_get_type(list, idx);


        /* if we have a file, add up its chunks */
        if (type == MFU_TYPE_FILE) {
            /* get size of file */
            uint64_t file_size = mfu_flist_file_get_size(list, idx);

	uint64_t chunks = file_size /chunk_size;
	uint64_t chunk_cnt = 0;
        if (chunks * chunk_size < file_size || file_size == 0) {
                chunks++;
            }
//	printf("rank %d idx %d # of chunks %d\n", rank, idx, chunks);

            /* get layout information */
            double start = MPI_Wtime();
            layout = llapi_layout_get_by_path(mfu_flist_file_get_name(list, idx), 0);
            double end = MPI_Wtime();
            llapi_record_timing(start, end, &llapi_timing_info);
//		printf("name: %s\n", mfu_flist_file_get_name(list, idx), 0); 
           if (layout == NULL){
                printf("errno: layout is NULL\n");
            }
            rc[0] = llapi_layout_comp_use(layout, 1);
            if (rc[0]){
                printf("error: layout component iteration failed\n");
            }
            while (1){
                        rc[0] = llapi_layout_stripe_count_get(layout, &cnt);
                        rc[1] = llapi_layout_stripe_size_get(layout, &ssize);
                        rc[2] = llapi_layout_comp_extent_get(layout, &start, &end);
                        if (rc[0] || rc[1] || rc[2]){
                            printf("error: cannot get stripe information\n");
                            continue;
                    	}
//			printf("cnt %d\n", cnt);
                    interval = cnt * ssize;
//			printf("interval %d\n", interval);
                    end = (end < file_size) ? end : file_size;
                    for (int i = 0; i < cnt; i++){
                        rc[0] = llapi_layout_ost_index_get(layout, i, &ost_idx);
                        if (rc[0]){
                                goto here_exit;
                        }
			int next_offset = 0;
			for(int j=0; next_offset <=end; j++){
//			printf("next_offset %d, rank: %d\n", next_offset, rank);
			int off = (start + i * ssize) + (j*interval);
//			if(off >= file_size && file_size != 0){
			if(chunk_cnt >= chunks){
				goto here_exit;
			}
                        mfu_file_chunk* elem = (mfu_file_chunk*) MFU_MALLOC(sizeof(mfu_file_chunk));
                        elem->name             = mfu_flist_file_get_name(list, idx);
                        elem->offset           = off;
			next_offset = (start+i*ssize)+((j+1)*interval);
                        elem->file_size        = file_size;
			elem->ost	       = ost_idx;
                        elem->rank_of_owner    = rank;
                        elem->index_of_owner   = idx;
                        elem->next             = NULL;
                        int task_ost = ost_idx;
			chunk_cnt++;
			if (file_size < chunk_size){
				elem -> length = file_size; 
			}
			else if ((elem->offset + chunk_size) > file_size){
				elem-> length = file_size - elem->offset;
			}
			else{
				elem->length = chunk_size;
			}
//			printf("file_size: %d, offset: %d\n", file_size, elem->offset); 
//			printf("length: %d\n", elem->length);
                        size_t pack_size = strlen(elem->name) + 1;
                        pack_size += 6 * 8;
                        int dest_rank, num_binded_worker;

                        if (OST_NUMBER >= worker_number){
                                dest_rank = task_ost % worker_number;
//				printf("task_ost: %d worker_num: %d dest_rank: %d\n", task_ost, worker_number, dest_rank);
			}
                        else{
                                int remainder = task_ost < (worker_number % OST_NUMBER) ? 1 : 0;
                                num_binded_worker = worker_number / OST_NUMBER + remainder;
                                dest_rank = (num_tasks_per_ost[task_ost] % num_binded_worker) * OST_NUMBER + task_ost;
                                num_tasks_per_ost[task_ost]++;
                        }
			if (dest_rank == rank){
				
				mfu_file_chunk* p = malloc(sizeof(mfu_file_chunk));
				p->next = NULL;
				p->name = strdup(elem->name);
				p->offset = off;
				p->file_size = file_size;
				p->length = elem->length;
				p->ost = ost_idx;
				p->rank_of_owner = rank;
				p->index_of_owner = idx;			
				
				if(head == NULL)
					head = p;
				if(tail != NULL)
					tail->next = p;
				tail= p;
			}
			else{
                        /* append element to list */
                        if (heads[dest_rank] == NULL) {
                            heads[dest_rank] = elem;
                        }
                        if (tails[dest_rank] != NULL) {
                            tails[dest_rank]->next = elem;
                       	} 
                        tails[dest_rank] = elem;
                        counts[dest_rank]++;
                        bytes[dest_rank] += pack_size;
			}
			} //end of inner for
                    } //end of for 
                   rc[0] = llapi_layout_comp_use(layout, 3);
                    if (rc[0] == 0)
                        continue;
                    if (rc[0] < 0)
                        printf("error: layout component iteration failed\n");
                        break;
                } //end of while
                here_exit:
			;		
        }
    }

    /* create storage to hold byte counts that we'll send
 *  *      * and receive, it would be best to use uint64_t here
 *   *           * but for that, we'd need to create a datatypen,
 *    *                * with an int, we should be careful we don't overflow */
    int* send_counts = (int*) MFU_MALLOC((size_t)ranks * sizeof(int));
    int* recv_counts = (int*) MFU_MALLOC((size_t)ranks * sizeof(int));
    memset(send_counts, 0, ranks * sizeof(int));
    memset(recv_counts, 0, ranks * sizeof(int));
    /* initialize our send counts */
    for (int i = 0; i < ranks; i++) {
        /* TODO: check that we don't overflow here */

        send_counts[i] = (i != rank && bytes[i] > 0) ? (int) bytes[i] : 0;
//	printf("bytes %d send counts %d rank %d\n", bytes[i], send_counts[i], rank);
    }


    /* exchange flags with ranks so everyone knows who they'll
 *      * receive data from */
    MPI_Alltoall(send_counts, 1, MPI_INT, recv_counts, 1, MPI_INT, MPI_COMM_WORLD);

    int total_sends = 0, total_recvs = 0;
    for (int i = 0; i < ranks; i++) {
        if (send_counts[i] > 0) total_sends++;
        if (recv_counts[i] > 0) total_recvs++;
//	printf("rank %d, from/to rank %d, send_counts %d recv_counts %d \n", rank, i, send_counts[i], recv_counts[i]);
    }

//sy insert

    int total_requests = total_sends + total_recvs;

    MPI_Request* request = (MPI_Request*) MFU_MALLOC((size_t)total_requests * sizeof(MPI_Request));
    MPI_Status*  status  = (MPI_Status*)  MFU_MALLOC((size_t)total_requests * sizeof(MPI_Status));

    /* post irecv to get sizes */
/*
int req_idx = 0;

for (int i = 0;i < ranks; i++) {
    if (i!= rank) {
        MPI_Irecv(&recv_counts[i], 1, MPI_INT, i, 0, MPI_COMM_WORLD, &request[req_idx++]);
        MPI_Irecv(&recv_counts[i], 1, MPI_INT, i, 0, MPI_COMM_WORLD, &request[i]);
    }
}

for (int i = 0; i < ranks; i++) {
    if (i != rank) { // Skip sending to itself
        MPI_Isend(&send_counts[i], 1, MPI_INT, i, 0, MPI_COMM_WORLD, &request[req_idx++]);
        MPI_Isend(&send_counts[i], 1, MPI_INT, i, 0, MPI_COMM_WORLD, &request[i]);
	}
}
total_requests=req_idx;
*/

    /* wait for sizes to come in */
//    MPI_Waitall(total_requests, request, status);

    /* allocate memory and encode lists for sending */
    for (int i = 0; i < ranks; i++) {
        /* allocate buffer for this destination */
	if (i != rank && bytes[i]>0){
        size_t sendbuf_size = (size_t) bytes[i];
        sendbufs[i] = (char*) MFU_MALLOC(sendbuf_size);

        /* pack data into buffer */
        char* sendptr = sendbufs[i];
        mfu_file_chunk* elem = heads[i];
        while (elem != NULL) {
            /* pack file name */
            strcpy(sendptr, elem->name);
            sendptr += strlen(elem->name) + 1;

            /* pack chunk id, count, and file size */
            mfu_pack_uint64(&sendptr, elem->offset);
            mfu_pack_uint64(&sendptr, elem->length);
            mfu_pack_uint64(&sendptr, elem->file_size);
            mfu_pack_uint64(&sendptr, elem->ost);
            mfu_pack_uint64(&sendptr, elem->rank_of_owner);
            mfu_pack_uint64(&sendptr, elem->index_of_owner);

            /* go to next element */
            elem = elem->next;
        }
	}
    }
    for(int i =0; i < ranks; i++){
        int listsize = mfu_file_chunk_list_size(heads[i]);
    //        printf("rank %d to %d list size %d\n", rank,i, listsize);
    }

        int li = mfu_file_chunk_list_size(head);
	
//        printf("rank %d list size %d\n", rank, li);
    /* sum up total bytes that we'll receive */
    size_t recvbuf_size = 0;
    for (int i = 0; i < ranks; i++) {
	if (i != rank){
        	recvbuf_size += (size_t) recv_counts[i];
	}
    }

    /* allocate memory for recvs */
    char* recvbuf = (char*) MFU_MALLOC(recvbuf_size);

    /* post irecv for incoming data */
    char* recvptr = recvbuf;

//int req_idx = 0;
//char** recv_buffers = (char**) MFU_MALLOC(ranks * sizeof(char*));
/*for (int i = 0; i < ranks; i++) {
    if (recv_counts[i] > 0) {
        recv_buffers[i] = (char*) MFU_MALLOC(recv_counts[i]);
    } else {
        recv_buffers[i] = NULL; // No data expected from this rank
    }
}


*/
int req_id = 0;
for (int i = 0; i < ranks; i++) {
    if (recv_counts[i] > 0) {
        MPI_Irecv(recvptr, recv_counts[i], MPI_BYTE, i, 0, MPI_COMM_WORLD, &request[req_id++]);
	recvptr += recv_counts[i];
    }
}

for (int i = 0; i < ranks; i++) {
    if (send_counts[i] > 0) {
        MPI_Isend(sendbufs[i], send_counts[i], MPI_BYTE, i, 0, MPI_COMM_WORLD, &request[req_id++]);
    }
}

    /* waitall */
    MPI_Waitall(total_requests, request, status);

//for(int i = 0; i < ranks; i++){
//	if(recv_counts[i] >0){
	
    /* iterate over all received data */
//    const char* packptr = recv_buffers[i];
    const char* packptr = recvbuf;
//    const char* recvbuf_end = packptr + recv_counts[i];
    char* recvbuf_end = recvbuf+recvbuf_size;
    while (packptr < recvbuf_end) {
        /* unpack file name */
        const char* name = packptr;
        packptr += strlen(name) + 1;

        /* unpack chunk offset, count, and file size */
        uint64_t offset, length, ost, file_size, rank_of_owner, index_of_owner;
        mfu_unpack_uint64(&packptr, &offset);
        mfu_unpack_uint64(&packptr, &length);
        mfu_unpack_uint64(&packptr, &file_size);
        mfu_unpack_uint64(&packptr, &ost);
        mfu_unpack_uint64(&packptr, &rank_of_owner);
        mfu_unpack_uint64(&packptr, &index_of_owner);

        /* allocate memory for new struct and set next pointer to null */
        mfu_file_chunk* p = malloc(sizeof(mfu_file_chunk));
//        mfu_file_chunk* p = (mfu_file_chunk*) MFU_MALLOC(sizeof(mfu_file_chunk));
        p->next = NULL;

        /* set the fields of the struct */
        p->name = strdup(name);
        p->offset = offset;
        p->length = length;
        p->file_size = file_size;
        p->ost = ost;
        p->rank_of_owner = rank_of_owner;
        p->index_of_owner = index_of_owner;

        /* if the tail is not null then point the tail at the latest struct */
        if (tail != NULL) {
            tail->next = p;
        }
        
        /* if head is not pointing at anything then this struct is head of list */
        if (head == NULL) {
            head = p;
        }

        /* have tail point at the current/last struct */
        tail = p;
    }
//	}
//}

	int listsize = mfu_file_chunk_list_size(head);
	printf("rank %d list size %d\n", rank, listsize);
    /* free the linked lists, packed send buffers, and related arrays */
    for (int i = 0; i < ranks; i++) {
        mfu_free(&sendbufs[i]);
 //   mfu_free(&recv_buffers[i]); // Free individual buffers
        /* free the element linked list for each rank.
 *          * Do not free elem->name because it is needed by the mfu_flist entry. */
        mfu_file_chunk* elem = heads[i];
        mfu_file_chunk* tmp;
        while (elem != NULL) {
            tmp = elem;
            elem = elem->next;
            mfu_free(&tmp);
        }
    }
    mfu_free(&heads);
    mfu_free(&tails);
    mfu_free(&counts);
    mfu_free(&bytes);
    mfu_free(&sendbufs);

    //mfu_free(&recv_buffers); // Free the array of pointers
    /* free the array for ranks recevied from */
    //mfu_free(&recvranklist);

    /* free the request and status messages */
    mfu_free(&request);
    mfu_free(&status);
    
    /* free the bytes counts arrays */
    mfu_free(&send_counts);
    mfu_free(&recv_counts);

    /* free the receive buffer */
    mfu_free(&recvbuf);

    return head;
}


/* free the linked list of structs (copy elem's) */
void mfu_file_chunk_list_free(mfu_file_chunk** phead)
{
    /* check whether we were given a pointer */
    if (phead != NULL) {
        /* free the linked list of structs (mfu_file_chunk) */
        mfu_file_chunk* tmp;
        mfu_file_chunk* current = *phead;
        while (current != NULL) {
            /* get pointer to current element and advance current */
            tmp = current;
            current = current->next;

            /* free the name string we had strdup'd */
            mfu_free(&tmp->name);

            /* free the element */
            mfu_free(&tmp);
        }

        /* set caller's pointer to NULL to indicate it's freed */
        *phead = NULL;
    }

    return;
}

uint64_t mfu_file_chunk_list_size(const mfu_file_chunk* p)
{
    uint64_t count = 0;
    while (p != NULL) {
        count++;
        p = p->next;
    }
    return count;
}

/* given an flist, a file chunk list generated from that flist,
 *  * and an input array of flags with one element per chunk,
 *   * execute a LOR per item in the flist, and return the result
 *    * to the process owning that item in the flist */
void mfu_file_chunk_list_lor(mfu_flist list, const mfu_file_chunk* head, const int* vals, int* results)
{
    /* get the largest filename */
    uint64_t max_name = mfu_flist_file_max_name(list);

	uint64_t file_list_size = mfu_flist_size(list);	

    /* if list is empty, we can't do much */
    if (max_name == 0) {
        return;
    }

    /* get a count of how many items are the chunk list */
    uint64_t list_count = mfu_file_chunk_list_size(head);
	printf("list_count: %d\n", list_count);
    /* keys are the filename, so only bytes that belong to 
 *      * the same file will be compared via a flag in the segmented scan */
    char* keys = (char*) MFU_MALLOC(list_count * max_name);

    /* ltr pointer for the output of the left-to-right-segmented scan */
    int* ltr = (int*) MFU_MALLOC(file_list_size * sizeof(int));

    /* copy file names into comparison buffer for segmented scan */
    uint64_t i;
    const mfu_file_chunk* p = head;
    for (i = 0; i < list_count; i++) {
        char* name = keys + max_name * i;
        strncpy(name, p->name, max_name);
        p = p->next;
    }

    /* create type and comparison operation for file names for the segmented scan */
    MPI_Datatype keytype = MPI_DATATYPE_NULL;
    DTCMP_Op keyop = DTCMP_OP_NULL;
    DTCMP_Str_create_ascend((int)max_name, &keytype, &keyop);

    /* execute segmented scan of comparison flags across file names */
    DTCMP_Segmented_scanv_ltr(
        (int)list_count, keys, keytype, keyop,
        vals, ltr, MPI_INT, MPI_LOR,
        DTCMP_FLAG_NONE, MPI_COMM_WORLD
    );
    
    /* we're done with the MPI type and operation, free them */
    MPI_Type_free(&keytype);
    DTCMP_Op_free(&keyop);

    /* get number of ranks */
    int ranks;
    MPI_Comm_size(MPI_COMM_WORLD, &ranks);

    /* allocate arrays for alltoall -- one for sending, and one for receiving */
    int* sendcounts = (int*) MFU_MALLOC((size_t)ranks * sizeof(int));
    int* recvcounts = (int*) MFU_MALLOC((size_t)ranks * sizeof(int));
    int* recvdisps  = (int*) MFU_MALLOC((size_t)ranks * sizeof(int));
    int* senddisps  = (int*) MFU_MALLOC((size_t)ranks * sizeof(int));

    /* allocate space for send buffer, we'll send an index value and comparison
 *      * flag, both as uint64_t */
    size_t sendbytes = file_list_size * 2 * sizeof(uint64_t); 
    uint64_t* sendbuf = (uint64_t*) MFU_MALLOC(sendbytes);

    /* initialize sendcounts array */
    for (int idx = 0; idx < ranks; idx++) {
        sendcounts[idx] = 0;
    }

    /* Iterate over the list of files. For each file a process needs to report on,
 *      * we increment the counter correspoinding to the "owner" of the file. After
 *           * going through all files, we then have a count of the number of files we 
 *                * will report for each rank */
    int disp = 0;
    p = head;
	int j = 0;
    for (i = 0; i < list_count; i++) {
        /* if we have the last byte of the file, we need to send scan result to owner */
        if (p->offset + p->length >= p->file_size) {
            /* increment count of items that will be sent to owner */
            int owner = (int) p->rank_of_owner;
            sendcounts[owner] += 2;

            /* copy index and flag value to send buffer */
            uint64_t file_index = p->index_of_owner;
            uint64_t flag       = (uint64_t) ltr[j++];
            sendbuf[disp    ]   = file_index;
            sendbuf[disp + 1]   = flag;
            
            /* advance to next value in buffer */
            disp += 2;
        }

        /* advance to next chunk */
        p = p->next;
    }

    /* compute send buffer displacements */
    senddisps[0] = 0;
    for (i = 1; i < (uint64_t)ranks; i++) {
        senddisps[i] = senddisps[i - 1] + sendcounts[i - 1];
    }

    /* alltoall to let every process know a count of how much it will be receiving */
    MPI_Alltoall(sendcounts, 1, MPI_INT, recvcounts, 1, MPI_INT, MPI_COMM_WORLD);

    /* calculate total incoming bytes and displacements for alltoallv */
    int recv_total = recvcounts[0];
    recvdisps[0] = 0;
    for (i = 1; i < (uint64_t)ranks; i++) {
        recv_total += recvcounts[i];
        recvdisps[i] = recvdisps[i - 1] + recvcounts[i - 1];
    }

    /* allocate buffer to recv bytes into based on recvounts */
    uint64_t* recvbuf = (uint64_t*) MFU_MALLOC((uint64_t)recv_total * sizeof(uint64_t));

    /* send the bytes to the correct rank that owns the file */
    MPI_Alltoallv(
        sendbuf, sendcounts, senddisps, MPI_UINT64_T,
        recvbuf, recvcounts, recvdisps, MPI_UINT64_T, MPI_COMM_WORLD
    );

    /* unpack contents of recv buffer & store results in strmap */
    disp = 0;
    while (disp < recv_total) {
        /* local store of idx & flag values for each file */
        uint64_t idx  = recvbuf[disp];
        uint64_t flag = recvbuf[disp + 1];

        /* set value in output array for corresponding item */
        results[idx] = (int)flag;

        /* go to next id & flag */
        disp += 2;
    }

    mfu_free(&recvbuf);
    mfu_free(&sendbuf);

    mfu_free(&sendcounts);
    mfu_free(&recvcounts);
    mfu_free(&recvdisps);
    mfu_free(&senddisps);

    mfu_free(&keys);
    mfu_free(&ltr);

    return;
}
