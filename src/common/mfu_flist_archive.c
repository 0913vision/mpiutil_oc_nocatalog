
/**
 * @file dtar.c - parallel tar main file
 *
 * @author - Feiyi Wang
 *
 *
 */

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#define _LARGEFILE64_SOURCE

#include <errno.h>
#include <stdio.h>
#include <stdbool.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <mpi.h>
#include <libcircle.h>
#include <archive.h>
#include <archive_entry.h>
#include <string.h>
#include <getopt.h>

#ifdef LUSTRE_SUPPORT
#include <lustre/lustreapi.h>
#endif

#include "mfu.h"

/* libcircle work operation types */
typedef enum {
    COPY_DATA /* copy data from user file into archive file */
} DTAR_operation_code_t;

/* struct to define a decoded work operation from libcircle */
typedef struct {
    uint64_t file_size;         /* size of user data file in bytes */
    uint64_t chunk_index;       /* chunk id of this work item, chunks are fixed size */
    uint64_t offset;            /* byte offset into archive file */
    DTAR_operation_code_t code; /* for now, just copy operation */
    char* operand;              /* full path to user data file */
} DTAR_operation_t;

/* defines state needed to write to the archive file */
typedef struct {
    const char* name;      /* file name of archive */
    int fd;                /* file descriptor of archive file */
    int flags;             /* flags used to open archive file */
    void* header_buf;      /* memory buffer in which to encode entry headers */
    size_t header_bufsize; /* size of memory buffer in bytes */
    void* io_buf;          /* memory buffer to read/write files */
    size_t io_bufsize;     /* size of memory i/o buffer in bytes */
} DTAR_writer_t;

DTAR_writer_t DTAR_writer; /* state of open archive file and I/O buffer */
mfu_flist DTAR_flist;      /* source flist of set of items being copied into archive */
uint64_t* DTAR_offsets        = NULL; /* byte offset into archive for each entry in our list */
uint64_t* DTAR_header_sizes   = NULL; /* byte size of header for each entry in our list */
mfu_archive_opts_t* DTAR_opts = NULL; /* pointer to archive options */

static int DTAR_err = 0; /* whether a process encounters an error while executing libcircle ops */

static void DTAR_abort(int code)
{
    MPI_Abort(MPI_COMM_WORLD, code);
    exit(code);
}

static void DTAR_exit(int code)
{
    mfu_finalize();
    MPI_Finalize();
    exit(code);
}

/****************************************
 * Cache opened files to avoid repeated open/close of
 * the same file when using libcicle
 ***************************************/

/* TODO: this is basically a copy of mfu_copy_file_open/close.
 * It should be moved to use that instead in the future */

/* cache open file descriptor to avoid
 * opening / closing the same file */
typedef struct {
    char* name; /* name of open file (NULL if none) */
    int   read; /* whether file is open for read-only (1) or write (0) */
    int   fd;   /* file descriptor */
} mfu_archive_file_cache_t;

/* close a file that opened with mfu_copy_open_file */
static int mfu_archive_close_file(
    mfu_archive_file_cache_t* cache)
{
    int rc = 0;

    /* close file if we have one */
    char* name = cache->name;
    if (name != NULL) {
        int fd = cache->fd;

        /* if open for write, fsync */
        int read_flag = cache->read;
        if (! read_flag) {
            rc = mfu_fsync(name, fd);
        }

        /* close the file and delete the name string */
        rc = mfu_close(name, fd);
        mfu_free(&cache->name);
    }

    return rc;
}

/* open and cache a file.
 * Returns 0 on success, -1 otherwise */
static int mfu_archive_open_file(
    const char* file,                /* path to file to be opened */
    int read_flag,                   /* set to 1 to open in read only, 0 for write */
    mfu_archive_file_cache_t* cache) /* cache the open file to avoid repetitive open/close of the same file */
{
    /* see if we have a cached file descriptor */
    char* name = cache->name;
    if (name != NULL) {
        /* we have a cached file descriptor */
        int fd = cache->fd;
        if (strcmp(name, file) == 0 && cache->read == read_flag) {
            /* the file we're trying to open matches name and read/write mode,
             * so just return the cached descriptor */
            return 0;
        } else {
            /* the file we're trying to open is different,
             * close the old file and delete the name */
            mfu_archive_close_file(cache);
        }
    }

    /* open the new file */
    int fd;
    if (read_flag) {
        int flags = O_RDONLY;
        fd = mfu_open(file, flags);
    } else {
        int flags = O_WRONLY | O_CREAT;
        fd = mfu_open(file, flags, DCOPY_DEF_PERMS_FILE);
    }
    if (fd < 0) {
        return -1;
    }

    /* cache the file descriptor */
    cache->name = MFU_STRDUP(file);
    cache->fd   = fd;
    cache->read = read_flag;

    return 0;
}

/** Cache most recent open file descriptor to avoid opening / closing the same file */
static mfu_archive_file_cache_t mfu_archive_src_cache;
static mfu_archive_file_cache_t mfu_archive_dst_cache;

/****************************************
 * Global variables used for extraction progress messages
 ***************************************/

mfu_progress* extract_prog = NULL;

#define REDUCE_BYTES (0)
#define REDUCE_ITEMS (1)
static uint64_t reduce_buf[2];

/****************************************
 * Global counter and callbacks for LIBCIRCLE reductions
 ***************************************/

/* holds total item count and byte count for reduction */
uint64_t DTAR_total_items = 0;
uint64_t DTAR_total_bytes = 0;

/* MPI_Wtime when operation was started and number of bytes written */
static double   reduce_start;
static uint64_t reduce_bytes;

static void reduce_init(void)
{
    CIRCLE_reduce(&reduce_bytes, sizeof(uint64_t));
}

static void reduce_exec(const void* buf1, size_t size1, const void* buf2, size_t size2)
{
    const uint64_t* a = (const uint64_t*) buf1;
    const uint64_t* b = (const uint64_t*) buf2;
    uint64_t val = a[0] + b[0];
    CIRCLE_reduce(&val, sizeof(uint64_t));
}

static void reduce_fini(const void* buf, size_t size)
{
    /* get result of reduction */
    const uint64_t* a = (const uint64_t*) buf;
    unsigned long long val = (unsigned long long) a[0];

    /* get current time */
    double now = MPI_Wtime();

    /* compute walk rate */
    double rate = 0.0;
    double secs = now - reduce_start;
    if (secs > 0.0) {
        rate = (double)val / secs;
    }

    /* convert total bytes to units */
    double val_tmp;
    const char* val_units;
    mfu_format_bytes(val, &val_tmp, &val_units);

    /* convert bandwidth to units */
    double rate_tmp;
    const char* rate_units;
    mfu_format_bw(rate, &rate_tmp, &rate_units);

    /* compute percentage done */
    double percent = 0.0;
    if (DTAR_total_bytes > 0) {
        percent = (double)val * 100.0 / (double)DTAR_total_bytes;
    }

    /* estimate seconds remaining */
    double secs_remaining = 0.0;
    if (rate > 0.0) {
        secs_remaining = (double)(DTAR_total_bytes - (uint64_t)val) / rate;
    }

    /* print status to stdout */
    MFU_LOG(MFU_LOG_INFO, "Tarred %.3lf %s (%.0f\%) in %.3lf secs (%.3lf %s) %.0f secs left ...",
        val_tmp, val_units, percent, secs, rate_tmp, rate_units, secs_remaining
    );
}

/* given an item name, and a working directory path,
 * compute relative path from working directory to the item
 * and return that relative path as a newly allocated string */
char* mfu_param_path_relative(
    const char* name,
    const mfu_param_path* cwdpath)
{
    /* create path of item */
    mfu_path* item = mfu_path_from_str(name);

    /* get current working directory */
    mfu_path* cwd = mfu_path_from_str(cwdpath->path);

    /* get relative path from current working dir to item */
    mfu_path* rel = mfu_path_relative(cwd, item);

    /* convert to a NUL-terminated string */
    char* dest = mfu_path_strdup(rel);

    /* free our temporary paths */
    mfu_path_delete(&rel);
    mfu_path_delete(&cwd);
    mfu_path_delete(&item);

    return dest;
}

/* given an entry in the flist, construct and encode its tar header
 * in the provided buffer, return number of bytes consumed in outsize */
static int encode_header(
    mfu_flist flist,               /* list of items */
    uint64_t idx,                  /* index of list item to be encoded */
    const mfu_param_path* cwdpath, /* current working dir to compute relative path to item */
    void* buf,                     /* buffer in which to store encoded header */
    size_t bufsize,                /* size of input buffer */
    mfu_archive_opts_t* opts,      /* archive options, which may affect encoding */
    size_t* outsize)               /* number of bytes consumed to encode header */
{
    /* assume we'll succeed */
    int rc = MFU_SUCCESS;

    /* allocate and entry for this item */
    struct archive_entry* entry = archive_entry_new();

    /* get file name for this item */
    const char* fname = mfu_flist_file_get_name(flist, idx);

    /* compute relative path to item from current working dir */
    const char* relname = mfu_param_path_relative(fname, cwdpath);
    archive_entry_copy_pathname(entry, relname);
    mfu_free(&relname);

    /* determine whether user wants to encode ACLs and xattrs */
    if (opts->preserve) {
        /* TODO: check that this still works */
        struct archive* source = archive_read_disk_new();
        archive_read_disk_set_standard_lookup(source);
        int fd = mfu_open(fname, O_RDONLY);
        if (archive_read_disk_entry_from_file(source, entry, fd, NULL) != ARCHIVE_OK) {
            MFU_LOG(MFU_LOG_ERR, "archive_read_disk_entry_from_file(): %s",
                archive_error_string(source)
            );
            rc = MFU_FAILURE;
        }
        archive_read_free(source);
        mfu_close(fname, fd);
    } else {
        /* TODO: read stat info from mfu_flist */
        struct stat stbuf;
        mfu_lstat(fname, &stbuf);
        archive_entry_copy_stat(entry, &stbuf);

        /* set user name of owner */
        const char* uname = mfu_flist_file_get_username(flist, idx);
        archive_entry_set_uname(entry, uname);

        /* set group name */
        const char* gname = mfu_flist_file_get_groupname(flist, idx);
        archive_entry_set_gname(entry, gname);

        /* if entry is a symlink, copy its target */
        mfu_filetype type = mfu_flist_file_get_type(flist, idx);
        if (type == MFU_TYPE_LINK) {
            /* got a symlink, read its target */
            char target[PATH_MAX + 1];              /* make space to add a trailing NUL */
            size_t targetsize = sizeof(target) - 1; /* leave space for a NUL */
            ssize_t readlink_rc = mfu_readlink(fname, target, targetsize);
            if (readlink_rc != -1) {
                /* readlink call succeeded, but check we didn't truncate the target */
                if (readlink_rc < (ssize_t)targetsize) {
                    /* got a target, but readlink doesn't null terminate,
                     * null terminate and copy into link field of entry */
                    target[readlink_rc] = '\0';
                    archive_entry_copy_symlink(entry, target);
                } else {
                    MFU_LOG(MFU_LOG_ERR, "Link target of `%s' exceeds buffer size %llu",
                        fname, targetsize
                    );
                    rc = MFU_FAILURE;
                }
            } else {
                MFU_LOG(MFU_LOG_ERR, "Failed to read link `%s' readlink() (errno=%d %s)",
                    fname, errno, strerror(errno)
                );
                rc = MFU_FAILURE;
            }
        }
    }

    /* write entry info to archive */
    struct archive* dest = archive_write_new();
    archive_write_set_format_pax(dest);

    /* don't buffer data, write everything directly to output (file or memory) */
    archive_write_set_bytes_per_block(dest, 0);

    /* encode entry into user's buffer */
    size_t used = 0;
    if (archive_write_open_memory(dest, buf, bufsize, &used) != ARCHIVE_OK) {
        MFU_LOG(MFU_LOG_ERR, "archive_write_open_memory(): %s",
            archive_error_string(dest)
        );
        rc = MFU_FAILURE;
    }

    /* write header for this item */
    if (archive_write_header(dest, entry) != ARCHIVE_OK) {
        MFU_LOG(MFU_LOG_ERR, "archive_write_header(): %s",
            archive_error_string(dest)
        );
        rc = MFU_FAILURE;
    }

    /* done with the entry object */
    archive_entry_free(entry);

    /* at this point, the used variable tells us the size of the header for this item */

    /* hack: mark the archive as failed, so that libarchive will not write
     * to the archive when we call free */
    archive_write_fail(dest);

    /* free resources associated with dest object */
    archive_write_free(dest);

    /* output size of header */
    *outsize = used;

    /* return size of header for this entry */
    return rc;
}

/* write header for specified item in flist to archive file */
static int write_header(
    mfu_flist flist,               /* flist holding item for which to write header */
    uint64_t idx,                  /* index of item in flist */
    const mfu_param_path* cwdpath, /* current working dir, to store relative path from cwd to item */
    void* buf,                     /* buffer in which to store encoded header */
    size_t bufsize,                /* size of input buffer */
    mfu_archive_opts_t* opts,      /* archive options, which may affect encoding */
    const char* filename,          /* filename of archive to write header to */
    int fd,                        /* open file descriptor to write header to */
    uint64_t offset)               /* byte offset in archive at which to write header */
{
    /* get name of item for any error messages */
    const char* name = mfu_flist_file_get_name(flist, idx);

    /* encode header for this entry in our buffer */
    size_t header_size;
    int encode_rc = encode_header(flist, idx, cwdpath,
        buf, bufsize, opts, &header_size
    );
    if (encode_rc != MFU_SUCCESS) {
        MFU_LOG(MFU_LOG_ERR, "Failed to encode header for `%s'",
            name);
        DTAR_err = 1;
        return MFU_FAILURE;
    }

    /* write header to archive for this entry */
    ssize_t pwrite_rc = mfu_pwrite(filename, fd, buf, header_size, offset);
    if (pwrite_rc == -1) {
        MFU_LOG(MFU_LOG_ERR, "Failed to write header for '%s' at offset %llu in archive file '%s' errno=%d %s",
            name, offset, filename, errno, strerror(errno));
        DTAR_err = 1;
        return MFU_FAILURE;
    }

    return MFU_SUCCESS;
}

/* construct a libcircle work item to copy a segment of a user file
 * into the archive */
static char* DTAR_encode_operation(
    DTAR_operation_code_t code, /* libcircle operation code (COPY_DATA) */
    const char* operand,        /* full path to user file */
    uint64_t fsize,             /* size of user file in bytes */
    uint64_t chunk_idx,         /* chunk id to be copied */
    uint64_t offset)            /* byte offset within archive to copy to */
{
    /* allocate a buffer to hold the encoded work item */
    size_t opsize = (size_t) CIRCLE_MAX_STRING_LEN;
    char* op = (char*) MFU_MALLOC(opsize);

    /* get length of user file name */
    size_t len = strlen(operand);

    /* encode work item as string */
    int written = snprintf(op, opsize,
        "%" PRIu64 ":%" PRIu64 ":%" PRIu64 ":%d:%d:%s",
        fsize, chunk_idx, offset, code, (int) len, operand);

    /* check that we don't truncate the string */
    if (written >= opsize) {
        MFU_LOG(MFU_LOG_ERR, "Exceed libcirlce message size");
        DTAR_abort(EXIT_FAILURE);
    }

    /* return work item encoded as string */
    return op;
}

/* given an encoded work item, decode into newly allocated a work structure */
static DTAR_operation_t* DTAR_decode_operation(char* op)
{
    /* allocate a new work structure */
    DTAR_operation_t* ret = (DTAR_operation_t*) MFU_MALLOC(sizeof(DTAR_operation_t));

    /* extract the file size */
    if (sscanf(strtok(op, ":"), "%" SCNu64, &(ret->file_size)) != 1) {
        MFU_LOG(MFU_LOG_ERR, "Could not decode file size attribute.");
        DTAR_abort(EXIT_FAILURE);
    }

    /* extract the chunk index */
    if (sscanf(strtok(NULL, ":"), "%" SCNu64, &(ret->chunk_index)) != 1) {
        MFU_LOG(MFU_LOG_ERR, "Could not decode chunk index attribute.");
        DTAR_abort(EXIT_FAILURE);
    }

    /* extract the offset into the archive file */
    if (sscanf(strtok(NULL, ":"), "%" SCNu64, &(ret->offset)) != 1) {
        MFU_LOG(MFU_LOG_ERR, "Could not decode source base offset attribute.");
        DTAR_abort(EXIT_FAILURE);
    }

    /* extract the work operation code (COPY_DATA) */
    if (sscanf(strtok(NULL, ":"), "%d", (int*) & (ret->code)) != 1) {
        MFU_LOG(MFU_LOG_ERR, "Could not decode stage code attribute.");
        DTAR_abort(EXIT_FAILURE);
    }

    /* get number of characters in operand string */
    int op_len;
    char* str = strtok(NULL, ":");
    if (sscanf(str, "%d", &op_len) != 1) {
        MFU_LOG(MFU_LOG_ERR, "Could not decode operand string length.");
        DTAR_abort(EXIT_FAILURE);
    }

    /* skip over digits and trailing ':' to get pointer to operand,
     * note that this just points to a substring of the input string
     * rather than allocate a new string */
    char* operand = str + strlen(str) + 1;
    operand[op_len] = '\0';
    ret->operand = operand;

    return ret;
}

/* iterate over all items in our flist and add libcircle work
 * items to copy chunks for any regular files */
static void DTAR_enqueue_copy(CIRCLE_handle* handle)
{
    /* insert copy operations for all chunks of each regular file
     * in our portion of the list */
    uint64_t idx;
    uint64_t listsize = mfu_flist_size(DTAR_flist);
    for (idx = 0; idx < listsize; idx++) {
        /* add copy work only for files */
        mfu_filetype type = mfu_flist_file_get_type(DTAR_flist, idx);
        if (type != MFU_TYPE_FILE) {
            /* not a regular file, skip this item */
            continue;
        }

        /* got a regular file, get name and its size */
        const char* name = mfu_flist_file_get_name(DTAR_flist, idx);
        uint64_t size = mfu_flist_file_get_size(DTAR_flist, idx);

        /* compute offset for first byte of file content */
        uint64_t doffset = DTAR_offsets[idx] + DTAR_header_sizes[idx];

        /* compute number of full chunks based on file size */
        uint64_t chunk_size = DTAR_opts->chunk_size;
        uint64_t num_chunks = size / chunk_size;

        /* insert a work item for each chunk */
        uint64_t chunk_idx;
        for (chunk_idx = 0; chunk_idx < num_chunks; chunk_idx++) {
            char* newop = DTAR_encode_operation(
                COPY_DATA, name, size, chunk_idx, doffset);
            handle->enqueue(newop);
            mfu_free(&newop);
        }

        /* create copy work for possibly last item */
        if (num_chunks * chunk_size < size || num_chunks == 0) {
            char* newop = DTAR_encode_operation(
                COPY_DATA, name, size, num_chunks, doffset);
            handle->enqueue(newop);
            mfu_free(&newop);
        }
    }
}

static void DTAR_perform_copy(CIRCLE_handle* handle)
{
    /* TODO: on error, should we call circle_abort to bail out? */

    /* dequeue next work item from libcircle */
    char opstr[CIRCLE_MAX_STRING_LEN];
    handle->dequeue(opstr);

    /* decode work item into a new work structure */
    DTAR_operation_t* op = DTAR_decode_operation(opstr);

    /* get name of user file */
    const char* in_name = op->operand;

    /* open input file for reading */
    int read = 1;
    int open_rc = mfu_archive_open_file(in_name, read, &mfu_archive_src_cache);
    int in_fd = mfu_archive_src_cache.fd;
    if (open_rc == -1) {
        MFU_LOG(MFU_LOG_ERR, "Failed to open source file '%s' errno=%d %s",
            in_name, errno, strerror(errno));
        DTAR_err = 1;
        in_fd = -1;
    }

    /* get name and opened file descriptor to archive file */
    const char* out_name = DTAR_writer.name;
    int out_fd = DTAR_writer.fd;

    /* file are sliced up in units of chunk_size bytes */
    uint64_t chunk_size = DTAR_opts->chunk_size;

    /* seek to proper offset in input file */
    uint64_t in_offset = chunk_size * op->chunk_index;
    off_t lseek_rc = mfu_lseek(in_name, in_fd, in_offset, SEEK_SET);
    if (lseek_rc == (off_t)-1) {
        MFU_LOG(MFU_LOG_ERR, "Failed to seek in source file '%s' errno=%d %s",
            in_name, errno, strerror(errno));
        DTAR_err = 1;
    }

    /* seek to position within archive file to write this data */
    uint64_t out_offset = op->offset + in_offset;
    lseek_rc = mfu_lseek(out_name, out_fd, out_offset, SEEK_SET);
    if (lseek_rc == (off_t)-1) {
        MFU_LOG(MFU_LOG_ERR, "Failed to seek in destination file '%s' errno=%d %s",
            out_name, errno, strerror(errno));
        DTAR_err = 1;
    }

    /* read data from input and write to archive */
    uint64_t total_bytes_written = 0;
    while (total_bytes_written < chunk_size && DTAR_err == 0) {
        /* compute number of bytes to read in this attempt */
        size_t num_to_read = DTAR_writer.io_bufsize;
        uint64_t remainder = chunk_size - total_bytes_written;
        if (remainder < (uint64_t) num_to_read) {
            num_to_read = (size_t) remainder;
        }

        /* read data from the source file */
        ssize_t nread = mfu_read(in_name, in_fd, DTAR_writer.io_buf, num_to_read);
        if (nread == 0) {
            /* hit end of file, we check below that we didn't end early */
            break;
        }
        if (nread == -1) {
            /* some form of read error */
            MFU_LOG(MFU_LOG_ERR, "Could not read '%s' errno=%d %s",
                in_name, errno, strerror(errno));
            DTAR_err = 1;
            break;
        }

        /* read some bytes, write out what we read */
        ssize_t nwritten = mfu_write(out_name, out_fd, DTAR_writer.io_buf, nread);
        if (nwritten != nread) {
            /* some form of write error */
            MFU_LOG(MFU_LOG_ERR, "Failed to write to '%s' errno=%d %s",
                out_name, errno, strerror(errno));
            DTAR_err = 1;
            break;
        }

        /* increment the number of bytes we've written so far */
        total_bytes_written += nwritten;
    }

    /* add bytes written into our reduce counter */
    reduce_bytes += total_bytes_written;

    /* compute index of last chunk in the file */
    uint64_t num_chunks = op->file_size / chunk_size;
    uint64_t rem = op->file_size - chunk_size * num_chunks;
    uint64_t last_chunk = (rem) ? num_chunks : num_chunks - 1;

    /* compute last offset we should have written to */
    uint64_t last_expected = in_offset + chunk_size;
    if (op->chunk_index == last_chunk) {
        last_expected = op->file_size;
    }

    /* check that we read all data we should have */
    uint64_t last_written = in_offset + total_bytes_written;
    if (last_written < last_expected) {
        MFU_LOG(MFU_LOG_ERR, "Failed to read all bytes of '%s' errno=%d %s",
            in_name, errno, strerror(errno));
        DTAR_err = 1;
    }
        
    /* if we're responsible for the end of the file, write NUL
     * to pad archive out to an integral multiple of 512 bytes */
    if (op->chunk_index == last_chunk) {
        /* we've got the last chunk, compute padding bytes */
        int padding = 512 - (int) (op->file_size % 512);
        if (padding > 0 && padding != 512) {
            /* need to pad, write out padding bytes of zero data */
            char buff[512] = {0};
            ssize_t nwritten = mfu_write(out_name, out_fd, buff, padding);
            if (nwritten != padding) {
                MFU_LOG(MFU_LOG_ERR, "Failed to write to '%s' errno=%d %s",
                    out_name, errno, strerror(errno));
                DTAR_err = 1;
            }
        }
    }

    /* release memory associated with work item */
    mfu_free(&op);
}

/* check that we got at least one readable path in source paths,
 * and check whether destination archive file already exists
 * and if not whether we can write to the parent directory */
void mfu_param_path_check_archive(
    int numparams,
    mfu_param_path* srcparams,
    mfu_param_path destparam,
    mfu_archive_opts_t* opts,
    int* valid)
{
    /* TODO: need to parallize this, rather than have every rank do the test */

    /* assume paths are valid */
    *valid = 1;

    /* count number of source paths that we can read */
    int i;
    int num_readable = 0;
    for (i = 0; i < numparams; i++) {
        char* path = srcparams[i].path;
        if (mfu_access(path, R_OK) == 0) {
            /* found one that we can read */
            num_readable++;
        } else {
            /* not readable, report using the verbatim string user specified */
            if (mfu_rank == 0) {
                char* orig = srcparams[i].orig;
                MFU_LOG(MFU_LOG_ERR, "Could not read '%s' errno=%d %s",
                    orig, errno, strerror(errno));
            }
        }
    }

    /* verify we have at least one valid source */
    if (num_readable < 1) {
        if (mfu_rank == 0) {
            MFU_LOG(MFU_LOG_ERR, "At least one valid source must be specified");
        }
        *valid = 0;
        goto bcast;
    }

    /* copy destination to user opts structure */
    opts->dest_path = MFU_STRDUP(destparam.path);

    /* check destination */
    if (destparam.path_stat_valid) {
        if (mfu_rank == 0) {
            MFU_LOG(MFU_LOG_WARN, "Destination target exists, we will overwrite");
        }
    } else {
        /* destination archive file does not exist,
         * check whether parent directory exists and is writable */

        /* compute path to parent of destination archive */
        mfu_path* parent = mfu_path_from_str(destparam.path);
        mfu_path_dirname(parent);
        char* parent_str = mfu_path_strdup(parent);
        mfu_path_delete(&parent);

        /* check if parent is writable */
        if (mfu_access(parent_str, W_OK) < 0) {
            if (mfu_rank == 0) {
                MFU_LOG(MFU_LOG_ERR, "Destination parent directory is not wriable: '%s' ",
                    parent_str
                );
            }
            *valid = 0;
            mfu_free(&parent_str);
            goto bcast;
        }

        mfu_free(&parent_str);
    }

    /* at this point, we know
     * (1) destination doesn't exist
     * (2) parent directory is writable
     */

bcast:
    MPI_Bcast(valid, 1, MPI_INT, 0, MPI_COMM_WORLD);

    if (! *valid) {
        if (mfu_rank == 0) {
            MFU_LOG(MFU_LOG_ERR, "Exiting run");
        }
        MPI_Barrier(MPI_COMM_WORLD);
        DTAR_exit(EXIT_FAILURE);
    }
}

/* Each process calls with the byte offset for each entry
 * it owns.  These are gathered in order and written into
 * an index file that is created for specified archive file. */
static int write_entry_index(
    const char* file,  /* name of archive file */
    uint64_t count,    /* number of items in offsets list */
    uint64_t* offsets) /* byte offset to each item in the archive file */
{
    /* compute file name of index file from archive file name */
    size_t namelen = strlen(file) + strlen(".idx") + 1;
    char* name = (char*) MFU_MALLOC(namelen);
    snprintf(name, namelen, "%s.idx", file);

    /* let user know what we're doing */
    if (mfu_debug_level >= MFU_LOG_VERBOSE && mfu_rank == 0) {
        MFU_LOG(MFU_LOG_INFO, "Writing index to %s", name);
    }

    /* compute total number of entries */
    uint64_t total;
    MPI_Allreduce(&count, &total, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);

    /* compute global offset of start of our entries */
    uint64_t offset;
    MPI_Scan(&count, &offset, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
    offset -= count;

    /* get number of ranks in our communicator */
    int ranks;
    MPI_Comm_size(MPI_COMM_WORLD, &ranks);

    /* allocate arrays to hold number of entries on each rank,
     * and displacement for each one */
    int* rank_counts = (int*) MFU_MALLOC(ranks * sizeof(int));
    int* rank_disps  = (int*) MFU_MALLOC(ranks * sizeof(int));

    /* gather counts and displacements to rank 0 */
    int count_int  = (int) count;
    int offset_int = (int) offset;
    MPI_Allgather(&count,  1, MPI_INT, rank_counts, 1, MPI_INT, MPI_COMM_WORLD);
    MPI_Allgather(&offset, 1, MPI_INT, rank_disps,  1, MPI_INT, MPI_COMM_WORLD);

    /* gather all items to rank 0 */
    uint64_t* all_offsets = NULL;
    if (mfu_rank == 0) {
        all_offsets = (uint64_t*) MFU_MALLOC(total * sizeof(uint64_t));
    }
    MPI_Gatherv(
        offsets, count_int, MPI_UINT64_T,
        all_offsets, rank_counts, rank_disps, MPI_UINT64_T,
        0, MPI_COMM_WORLD
    );
    
    /* have rank 0 write the file */
    int success = 1;
    if (mfu_rank == 0) {
        int fd = mfu_open(name, O_WRONLY | O_CREAT | O_TRUNC, 0660);
        if (fd >= 0) {
            /* compute size of memory buffer holding offsets */
            size_t bufsize = total * sizeof(uint64_t);
    
            /* pack offset values in network order */
            uint64_t i;
            uint64_t* packed = (uint64_t*) MFU_MALLOC(bufsize);
            char* ptr = (char*) packed;
            for (i = 0; i < total; i++) {
                mfu_pack_uint64(&ptr, all_offsets[i]);
            }
    
            /* write offsets to the index file */
            ssize_t nwritten = mfu_write(name, fd, packed, bufsize);
            if (nwritten != (ssize_t) bufsize) {
                /* failed to write to the file */
                MFU_LOG(MFU_LOG_ERR, "Failed to write index '%s' errno=%d %s",
                    name, errno, strerror(errno)
                );
                success = 0;
            }

            /* close the file */
            mfu_close(name, fd);

            /* free buffer allocaed to hold packed offsets */
            mfu_free(&packed);
        } else {
            /* failed to open the file */
            MFU_LOG(MFU_LOG_ERR, "Failed to open index '%s' errno=%d %s",
                name, errno, strerror(errno)
            );
            success = 0;
        }
    }

    /* free memory buffers */
    mfu_free(&all_offsets);
    mfu_free(&rank_counts);
    mfu_free(&rank_disps);
    mfu_free(&name);

    /* determine whether everyone succeeded */
    success = mfu_alltrue(success, MPI_COMM_WORLD);

    if (! success) {
        return MFU_FAILURE;
    }
    return MFU_SUCCESS;
}

/* attempts to read index for specified archive file name,
 * returns MFU_SUCCESS if successful, MFU_FAILURE otherwise,
 * on success, returns total number of entries in out_count,
 * and an allocated array of offsets in out_offsets */
static int read_entry_index(
    const char* filename,
    uint64_t* out_count,
    uint64_t** out_offsets)
{
    /* assume we'll succeed */
    int rc = MFU_SUCCESS;

    /* assume we have the index file */
    int have_index = 1;

    /* compute file name of index file */
    size_t namelen = strlen(filename) + strlen(".idx") + 1;
    char* name = (char*) MFU_MALLOC(namelen);
    snprintf(name, namelen, "%s.idx", filename);

    /* TODO: use a better encoding with index format
     * version number */

    /* compute number of entries based on file size */
    uint64_t count = 0;
    if (mfu_rank == 0) {
        struct stat st;
        int stat_rc = mfu_stat(name, &st);
        if (stat_rc == 0) {
            /* index stores one offset as uint64_t for each entry */
            count = st.st_size / sizeof(uint64_t);
        } else {
            /* failed to stat the index file,
             * don't bother with an error since this likely
             * means the index doesn't exist because the archive
             * was created with something other than mpiFileUtils */
            have_index = 0;
        }
    }

    /* broadcast number of entries to all ranks */
    MPI_Bcast(&count, 1, MPI_UINT64_T, 0, MPI_COMM_WORLD);

    /* read entry offsets from file */
    size_t bufsize = count * sizeof(uint64_t);
    uint64_t* offsets = (uint64_t*) MFU_MALLOC(bufsize);
    if (mfu_rank == 0 && have_index) {
        int fd = mfu_open(name, O_RDONLY);
        if (fd >= 0) {
            ssize_t nread = mfu_read(name, fd, offsets, bufsize);
            if (nread != bufsize) {
                /* have index file, but failed to read it */
                MFU_LOG(MFU_LOG_ERR, "Failed to read index '%s' errno=%d %s",
                    name, errno, strerror(errno)
                );
                have_index = 0;
            }
            mfu_close(name, fd);
        } else {
            /* failed to open index file */
            MFU_LOG(MFU_LOG_ERR, "Failed to open index '%s' errno=%d %s",
                name, errno, strerror(errno)
            );
            have_index = 0;
        }
    }

    /* broadcast whether rank 0 could stat index file */
    MPI_Bcast(&have_index, 1, MPI_UINT64_T, 0, MPI_COMM_WORLD);

    /* bail out if we done have an index file */
    if (! have_index) {
        /* no index file, free memory and return failure */
        mfu_free(&offsets);
        mfu_free(&name);
        return MFU_FAILURE;
    }

    /* indicate to user what phase we're in */
    if (mfu_rank == 0) {
        MFU_LOG(MFU_LOG_INFO, "Read index %s", name);
    }

    /* convert offsets into host order */
    uint64_t i;
    uint64_t* packed = (uint64_t*) MFU_MALLOC(bufsize);
    const char* ptr = (const char*) offsets;
    for (i = 0; i < count; i++) {
        mfu_unpack_uint64(&ptr, &packed[i]);
    }

    /* free offsets we read from file */
    mfu_free(&offsets);

    /* broadcast offsets to all ranks */
    MPI_Bcast(packed, count, MPI_UINT64_T, 0, MPI_COMM_WORLD);

    /* free name of index file */
    mfu_free(&name);

    /* return count and list of offsets */
    *out_count   = count;
    *out_offsets = packed;

    return rc; 
}

/* set lustre stripe parameters on a file */
static void mfu_set_stripes(
    const char* file,    /* path of file to be striped */
    const char* cwd,     /* current working dir to prepend to file if not absolute */
    size_t stripe_bytes, /* width of a single stripe in bytes */
    int stripe_count)    /* number of stripes, -1 for all */
{
    /* get our rank */
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    /* if file is on lustre, set striping parameters */
    if (rank == 0) {
        /* get absoluate path to file */
        mfu_path* dirpath = mfu_path_from_str(file);
        if (! mfu_path_is_absolute(dirpath)) {
            mfu_path_prepend_str(dirpath, cwd);
        }
        mfu_path_reduce(dirpath);

        /* get full path of item */
        const char* name = mfu_path_strdup(dirpath);

        /* get parent directory of item */
        mfu_path_dirname(dirpath);
        const char* dir = mfu_path_strdup(dirpath);

        /* if path is in lustre, configure the stripe parameters */
        if (mfu_is_lustre(dir)) {
            /* delete file incase it already exists
             * to reassign existing stripe settings */
            mfu_unlink(name);

            /* set striping parameters */
            mfu_stripe_set(name, stripe_bytes, stripe_count);
        }

        /* free temporary buffers */
        mfu_free(&name);
        mfu_free(&dir);
        mfu_path_delete(&dirpath);
    }

    /* wait for rank 0 to set the striping parameters */
    MPI_Barrier(MPI_COMM_WORLD);

    return;
}

/* write items in given flist to specified archive file
 * using libcircle to distribute the task of copying file data */
static int mfu_flist_archive_create_libcircle(
    mfu_flist inflist,             /* file list to be archived */
    const char* filename,          /* name of archive file */
    int numpaths,                  /* number of source paths used to generate file list */
    const mfu_param_path* paths,   /* list of source paths used to generate file list */
    const mfu_param_path* cwdpath, /* current working directory used to compute relative path to each item */
    mfu_archive_opts_t* opts)      /* options to configure archive operation */
{
    /* assume we'll succeed */
    int rc = MFU_SUCCESS;

    /* print note about what we're doing and the amount of files/data to be moved */
    if (mfu_debug_level >= MFU_LOG_VERBOSE && mfu_rank == 0) {
        MFU_LOG(MFU_LOG_INFO, "Writing archive to %s", filename);
    }

    /* print summary of item and byte count of items to be archived */
    mfu_flist_print_summary(inflist);

    /* start overall timer */
    time_t time_started;
    time(&time_started);
    double wtime_started = MPI_Wtime();

    /* sort items alphabetically, so they are placed in the archive with parent directories
     * coming before their children */
    mfu_flist flist = mfu_flist_sort("name", inflist);

    /* copy handles to objects into global variables used in libcircle callback functions */
    DTAR_flist = flist;
    DTAR_opts  = opts;

    /* we flip this to 1 if any process hits any error writing the archive */
    DTAR_err = 0;

    /* if archive file will be on lustre, set max striping since this should be big */
    mfu_set_stripes(filename, cwdpath->path, opts->chunk_size, -1);

    /* create the archive file */
    DTAR_writer.name  = filename;
    DTAR_writer.flags = O_WRONLY | O_CREAT | O_CLOEXEC | O_LARGEFILE;
    DTAR_writer.fd    = mfu_open(filename, DTAR_writer.flags, 0664);
    if (DTAR_writer.fd < 0) {
        MFU_LOG(MFU_LOG_ERR, "Failed to open archive '%s' errno=%d %s",
            DTAR_writer.name, errno, strerror(errno));
        DTAR_err = 1;
    }

    /* Allocate a buffer to encode tar headers.
     * The entire header must fit in this buffer.
     * Typical entries will have no problems, but we may exhaust
     * space for entries that have very long ACLs or XATTRs. */
    DTAR_writer.header_bufsize = 128 * 1024 * 1024;
    DTAR_writer.header_buf = MFU_MALLOC(DTAR_writer.header_bufsize);

    /* allocate buffer to read/write data */
    DTAR_writer.io_bufsize = opts->buf_size;
    DTAR_writer.io_buf = MFU_MALLOC(DTAR_writer.io_bufsize);

    /* get number of items in our portion of the list */
    uint64_t listsize = mfu_flist_size(flist);

    /* allocate memory for file sizes, offsets, and header sizes
     * for each of our items */
    uint64_t* fsizes  = (uint64_t*) MFU_MALLOC(listsize * sizeof(uint64_t));
    DTAR_offsets      = (uint64_t*) MFU_MALLOC(listsize * sizeof(uint64_t));
    DTAR_header_sizes = (uint64_t*) MFU_MALLOC(listsize * sizeof(uint64_t));

    /* compute local offsets for each item and total
     * bytes we're contributing to the archive */
    uint64_t idx;
    uint64_t bytes = 0;
    uint64_t data_bytes = 0;
    for (idx = 0; idx < listsize; idx++) {
        /* assume the item takes no space */
        DTAR_header_sizes[idx] = 0;
        fsizes[idx] = 0;

        /* identify item type to compute its size in the archive */
        mfu_filetype type = mfu_flist_file_get_type(flist, idx);
        if (type == MFU_TYPE_DIR || type == MFU_TYPE_LINK) {
            /* directories and symlinks only need the header */
            uint64_t header_size;
            encode_header(flist, idx, cwdpath,
                DTAR_writer.header_buf, DTAR_writer.header_bufsize, opts, &header_size);
            DTAR_header_sizes[idx] = header_size;
            fsizes[idx] = header_size;
        } else if (type == MFU_TYPE_FILE) {
            /* regular file requires a header, plus file content,
             * and things are packed into blocks of 512 bytes */
            uint64_t header_size;
            encode_header(flist, idx, cwdpath,
                DTAR_writer.header_buf, DTAR_writer.header_bufsize, opts, &header_size);
            DTAR_header_sizes[idx] = header_size;

            /* get file size of this item */
            uint64_t fsize = mfu_flist_file_get_size(flist, idx);

            /* round file size up to nearest integer number of 512 bytes */
            uint64_t fsize_padded = fsize / 512;
            fsize_padded *= 512;
            if (fsize_padded < fsize) {
                fsize_padded += 512;
            }

            /* entry size is the haeder plus the file data with padding */
            uint64_t entry_size = header_size + fsize_padded;
            fsizes[idx] = entry_size;

            /* increment our total data bytes */
            data_bytes += fsize_padded;
        } else {
            /* TODO: print warning about unsupported type? */
        }

        /* increment our local offset for this item */
        DTAR_offsets[idx] = bytes;
        bytes += fsizes[idx];
    }

    /* get total number of items and get total data byte count (plus padding) */
    DTAR_total_items = mfu_flist_global_size(flist);
    MPI_Allreduce(&data_bytes, &DTAR_total_bytes, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);

    /* compute total archive size */
    uint64_t archive_size = 0;
    MPI_Allreduce(&bytes, &archive_size, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);

    /* execute scan to figure our global offset in the archive file */
    uint64_t global_offset = 0;
    MPI_Scan(&bytes, &global_offset, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
    global_offset -= bytes;

    /* update offsets for each of our file to their global offset */
    for (idx = 0; idx < listsize; idx++) {
        DTAR_offsets[idx] += global_offset;
    }

    /* record global offsets in index */
    write_entry_index(filename, listsize, DTAR_offsets);

    /* print message to user that we're starting */
    if (mfu_debug_level >= MFU_LOG_VERBOSE && mfu_rank == 0) {
        MFU_LOG(MFU_LOG_INFO, "Truncating archive");
    }

    /* truncate file to correct size to overwrite existing file
     * and to preallocate space on the file system */
    if (mfu_rank == 0) {
        /* truncate to 0 to delete any existing file contents */
        mfu_ftruncate(DTAR_writer.fd, 0);

        /* truncate to proper size and preallocate space,
         * archive size represents the space to hold all entries,
         * then add on final two 512-blocks that mark the end of the archive */
        off_t final_size = archive_size + 2 * 512;
        mfu_ftruncate(DTAR_writer.fd, final_size);
        posix_fallocate(DTAR_writer.fd, 0, final_size);
    }
    MPI_Barrier(MPI_COMM_WORLD);

    /* print message to user that we're starting */
    if (mfu_debug_level >= MFU_LOG_VERBOSE && mfu_rank == 0) {
        MFU_LOG(MFU_LOG_INFO, "Writing entry headers");
    }

    /* write headers for our files */
    for (idx = 0; idx < listsize; idx++) {
        /* we currently only support regular files, directories, and symlinks */
        mfu_filetype type = mfu_flist_file_get_type(flist, idx);
        if (type == MFU_TYPE_FILE || type == MFU_TYPE_DIR || type == MFU_TYPE_LINK) {
            /* write header for this item to the archive,
             * this sets DTAR_err on any error */
            write_header(flist, idx, cwdpath,
                DTAR_writer.header_buf, DTAR_writer.header_bufsize, opts,
                DTAR_writer.name, DTAR_writer.fd, DTAR_offsets[idx]);
        } else {
            /* print a warning that we did not archive this item */
            const char* item_name = mfu_flist_file_get_name(flist, idx);
            MFU_LOG(MFU_LOG_WARN, "Unsupported type, cannot archive `%s'", item_name);
        }
    }

    /* print message to user that we're starting */
    if (mfu_debug_level >= MFU_LOG_VERBOSE && mfu_rank == 0) {
        MFU_LOG(MFU_LOG_INFO, "Copying file data");
    }

    /* initialize file cache for opening source files */
    mfu_archive_src_cache.name = NULL;

    /* prepare libcircle */
    CIRCLE_init(0, NULL, CIRCLE_SPLIT_EQUAL | CIRCLE_CREATE_GLOBAL | CIRCLE_TERM_TREE);
    CIRCLE_loglevel loglevel = CIRCLE_LOG_WARN;
    CIRCLE_enable_logging(loglevel);

    /* register callbacks */
    CIRCLE_cb_create(&DTAR_enqueue_copy);
    CIRCLE_cb_process(&DTAR_perform_copy);

    /* prepare callbacks and initialize variables for reductions */
    reduce_start = MPI_Wtime();
    reduce_bytes = 0;
    CIRCLE_cb_reduce_init(&reduce_init);
    CIRCLE_cb_reduce_op(&reduce_exec);
    CIRCLE_cb_reduce_fini(&reduce_fini);

    /* set libcircle reduction period */
    int reduce_secs = 0;
    if (mfu_progress_timeout > 0) {
        reduce_secs = mfu_progress_timeout;
    }
    CIRCLE_set_reduce_period(reduce_secs);

    /* run the libcircle job to copy data into archive file */
    CIRCLE_begin();
    CIRCLE_finalize();

    /* done writing, close any source file that is still open */
    mfu_archive_close_file(&mfu_archive_src_cache);

    /* rank 0 finalizes the archive by writing two 512-byte blocks of NUL
     * (according to tar file format) */
    if (mfu_rank == 0) {
        /* write two blocks of 512 bytes of 0 at end of archive */
        char buf[1024] = {0};
        size_t bufsize = sizeof(buf);
        ssize_t pwrite_rc = mfu_pwrite(DTAR_writer.name, DTAR_writer.fd, buf, bufsize, archive_size);
        if (pwrite_rc != bufsize) {
            MFU_LOG(MFU_LOG_ERR, "Failed to write to archive '%s' at offset %llu errno=%d %s",
                DTAR_writer.name, archive_size, errno, strerror(errno));
            DTAR_err = 1;
        }

        /* include final NULL blocks in our stats */
        archive_size += bufsize;
    }

    /* TODO: sync archive? */

    /* close archive file */
    int close_rc = mfu_close(DTAR_writer.name, DTAR_writer.fd);
    if (close_rc == -1) {
        MFU_LOG(MFU_LOG_ERR, "Failed to close archive '%s' errno=%d %s",
            DTAR_writer.name, errno, strerror(errno));
        DTAR_err = 1;
    }

    /* determine whether everyone succeeded in writing their part */
    int write_success = mfu_alltrue(DTAR_err == 0, MPI_COMM_WORLD);
    if (! write_success) {
        rc = MFU_FAILURE;
    }

    /* wait for all ranks to finish */
    MPI_Barrier(MPI_COMM_WORLD);

    /* free sorted list */
    mfu_free(&flist);

    /* stop overall time */
    time_t time_ended;
    time(&time_ended);
    double wtime_ended = MPI_Wtime();

    /* print stats */
    double secs = wtime_ended - wtime_started;
    if (mfu_rank == 0) {
        char starttime_str[256];
        struct tm* localstart = localtime(&time_started);
        strftime(starttime_str, 256, "%b-%d-%Y, %H:%M:%S", localstart);

        char endtime_str[256];
        struct tm* localend = localtime(&time_ended);
        strftime(endtime_str, 256, "%b-%d-%Y, %H:%M:%S", localend);

        /* convert size to units */
        double size_tmp;
        const char* size_units;
        mfu_format_bytes(archive_size, &size_tmp, &size_units);

        /* convert bandwidth to unit */
        double agg_rate_tmp;
        double agg_rate = (double)archive_size / secs;
        const char* agg_rate_units;
        mfu_format_bw(agg_rate, &agg_rate_tmp, &agg_rate_units);

        MFU_LOG(MFU_LOG_INFO, "Started:   %s", starttime_str);
        MFU_LOG(MFU_LOG_INFO, "Completed: %s", endtime_str);
        MFU_LOG(MFU_LOG_INFO, "Seconds: %.3lf", secs);
        MFU_LOG(MFU_LOG_INFO, "Archive size: %.3lf %s", size_tmp, size_units);
        MFU_LOG(MFU_LOG_INFO, "Rate: %.3lf %s " \
                "(%.3" PRIu64 " bytes in %.3lf seconds)", \
                agg_rate_tmp, agg_rate_units, archive_size, secs);
    }

    /* clean up */
    mfu_free(&DTAR_writer.header_buf);
    mfu_free(&DTAR_writer.io_buf);
    mfu_free(&fsizes);
    mfu_free(&DTAR_offsets);
    mfu_free(&DTAR_header_sizes);

    return rc;
}

/* progress message to print while setting file metadata */
static void create_progress_fn(const uint64_t* vals, int count, int complete, int ranks, double secs)
{
    /* compute average rate */
    double byte_rate = 0.0;
    if (secs > 0) {
        byte_rate = (double)vals[REDUCE_BYTES] / secs;
    }

    /* format number of bytes for printing */
    double bytes_val = 0.0;
    const char* bytes_units = NULL;
    mfu_format_bytes(vals[REDUCE_BYTES], &bytes_val, &bytes_units);

    /* format bandwidth for printing */
    double bw_val = 0.0;
    const char* bw_units = NULL;
    mfu_format_bw(byte_rate, &bw_val, &bw_units);

    /* compute percentage of bytes extracted */
    double percent = 0.0;
    if (DTAR_total_bytes > 0) {
        percent = (double)vals[REDUCE_BYTES] * 100.0 / (double)DTAR_total_bytes;
    }

    /* estimate seconds remaining */
    double secs_remaining = 0.0;
    if (byte_rate > 0.0) {
        secs_remaining = (double)(DTAR_total_bytes - vals[REDUCE_BYTES]) / byte_rate;
    }

    if (complete < ranks) {
        MFU_LOG(MFU_LOG_INFO,
            "Tarred %.3lf %s (%.0f\%) in %.3lf secs (%.3lf %s) %.0f secs left ...",
            bytes_val, bytes_units, percent, secs, bw_val, bw_units, secs_remaining
        );
    } else {
        MFU_LOG(MFU_LOG_INFO,
            "Tarred %.3lf %s (%.0f\%) in %.3lf secs (%.3lf %s) done",
            bytes_val, bytes_units, percent, secs, bw_val, bw_units
        );
    }
}

static int mfu_flist_archive_create_chunk(
    mfu_flist inflist,
    const char* filename,
    int numpaths,
    const mfu_param_path* paths,
    const mfu_param_path* cwdpath,
    mfu_archive_opts_t* opts)
{
    int rc = MFU_SUCCESS;

    /* print note about what we're doing and the amount of files/data to be moved */
    if (mfu_debug_level >= MFU_LOG_VERBOSE && mfu_rank == 0) {
        MFU_LOG(MFU_LOG_INFO, "Writing archive to %s", filename);
    }

    /* print summary of item and byte count of items to be archived */
    mfu_flist_print_summary(inflist);

    /* start overall timer */
    time_t time_started;
    time(&time_started);
    double wtime_started = MPI_Wtime();

    /* sort items alphabetically, so they are placed in the archive with parent directories
     * coming before their children */
    mfu_flist flist = mfu_flist_sort("name", inflist);

    /* we'll flip this to 1 if any process hits any error writing the archive */
    DTAR_err = 0;

    /* if archive file will be on lustre, set max striping since this should be big */
    mfu_set_stripes(filename, cwdpath->path, opts->chunk_size, -1);

    /* create the archive file */
    int flags = O_WRONLY | O_CREAT | O_CLOEXEC | O_LARGEFILE;
    int fd    = mfu_open(filename, flags, 0664);
    if (fd < 0) {
        MFU_LOG(MFU_LOG_ERR, "Failed to open archive '%s' errno=%d %s",
            filename, errno, strerror(errno));
        DTAR_err = 1;
    }

//    int lock_rc = llapi_group_lock(fd, 23);

    /* Allocate a buffer to encode tar headers.
     * The entire header must fit in this buffer.
     * Typical entries will have no problems, but we may exhaust
     * space for entries that have very long ACLs or XATTRs. */
    size_t header_bufsize = 128 * 1024 * 1024;
    void* header_buf = MFU_MALLOC(header_bufsize);

    /* get number of items in our portion of the list */
    uint64_t listsize = mfu_flist_size(flist);

    /* allocate memory for file sizes and offsets */
    uint64_t* header_sizes = (uint64_t*) MFU_MALLOC(listsize * sizeof(uint64_t));
    uint64_t* entry_sizes  = (uint64_t*) MFU_MALLOC(listsize * sizeof(uint64_t));
    uint64_t* offsets      = (uint64_t*) MFU_MALLOC(listsize * sizeof(uint64_t));
    uint64_t* data_offsets = (uint64_t*) MFU_MALLOC(listsize * sizeof(uint64_t));

    /* allocate buffer to read/write data */
    size_t bufsize = opts->buf_size;
    void* buf = MFU_MALLOC(bufsize);

    /* compute local offsets for each item and total
     * bytes we're contributing to the archive */
    uint64_t idx;
    uint64_t offset = 0;
    uint64_t data_bytes = 0;
    for (idx = 0; idx < listsize; idx++) {
        /* assume the item takes no space */
        header_sizes[idx] = 0;
        entry_sizes[idx]  = 0;

        /* identify item type to compute its size in the archive */
        mfu_filetype type = mfu_flist_file_get_type(flist, idx);
        if (type == MFU_TYPE_DIR || type == MFU_TYPE_LINK) {
            /* directories and symlinks only need the header */
            uint64_t header_size;
            encode_header(flist, idx, cwdpath,
                header_buf, header_bufsize, opts, &header_size);
            header_sizes[idx] = header_size;
            entry_sizes[idx]  = header_size;
        } else if (type == MFU_TYPE_FILE) {
            /* regular file requires a header, plus file content,
             * and things are packed into blocks of 512 bytes */
            uint64_t header_size;
            encode_header(flist, idx, cwdpath,
                header_buf, header_bufsize, opts, &header_size);
            header_sizes[idx] = header_size;

            /* get file size of this item */
            uint64_t fsize = mfu_flist_file_get_size(flist, idx);

            /* round file size up to nearest integer number of 512 bytes */
            uint64_t fsize_padded = fsize / 512;
            fsize_padded *= 512;
            if (fsize_padded < fsize) {
                fsize_padded += 512;
            }

            /* entry size is the haeder plus the file data with padding */
            uint64_t entry_size = header_size + fsize_padded;
            entry_sizes[idx] = entry_size;

            /* increment our total data bytes */
            data_bytes += fsize_padded;
        }

        /* increment our local offset for this item */
        offsets[idx] = offset;
        offset += entry_sizes[idx];
    }

    /* store total item and data byte count */
    uint64_t total_items = mfu_flist_global_size(flist);
    MPI_Allreduce(&data_bytes, &DTAR_total_bytes, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);

    /* compute total archive size */
    uint64_t archive_size = 0;
    MPI_Allreduce(&offset, &archive_size, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);

    /* execute scan to figure our global base offset in the archive file */
    uint64_t global_offset = 0;
    MPI_Scan(&offset, &global_offset, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
    global_offset -= offset;

    /* update offsets for each of our file to their global offset */
    for (idx = 0; idx < listsize; idx++) {
        offsets[idx] += global_offset;
        data_offsets[idx] = offsets[idx] + header_sizes[idx];
    }

    /* record global offsets in index */
    write_entry_index(filename, listsize, offsets);

    /* print message to user that we're starting */
    if (mfu_debug_level >= MFU_LOG_VERBOSE && mfu_rank == 0) {
        MFU_LOG(MFU_LOG_INFO, "Truncating archive");
    }

    /* truncate file to correct size to overwrite existing file
     * and to preallocate space on the file system */
    if (mfu_rank == 0) {
        /* truncate to 0 to delete any existing file contents */
        mfu_ftruncate(fd, 0);

        /* truncate to proper size and preallocate space,
         * archive size represents the space to hold all entries,
         * then add on final two 512-blocks that mark the end of the archive */
        off_t final_size = archive_size + 2 * 512;
        mfu_ftruncate(fd, final_size);
        posix_fallocate(fd, 0, final_size);
    }
    MPI_Barrier(MPI_COMM_WORLD);

    /* print message to user that we're starting */
    if (mfu_debug_level >= MFU_LOG_VERBOSE && mfu_rank == 0) {
        MFU_LOG(MFU_LOG_INFO, "Writing entry headers");
    }

    /* write headers for our files */
    for (idx = 0; idx < listsize; idx++) {
        /* we currently only support regular files, directories, and symlinks */
        mfu_filetype type = mfu_flist_file_get_type(flist, idx);
        if (type == MFU_TYPE_FILE || type == MFU_TYPE_DIR || type == MFU_TYPE_LINK) {
            /* write header for this item to the archive,
             * this sets DTAR_err on any error */
            write_header(flist, idx, cwdpath,
                header_buf, header_bufsize, opts,
                filename, fd, offsets[idx]);
        } else {
            /* print a warning that we did not archive this item */
            const char* item_name = mfu_flist_file_get_name(flist, idx);
            MFU_LOG(MFU_LOG_WARN, "Unsupported type, cannot archive `%s'", item_name);
        }
    }

    /* print message to user that we're starting */
    if (mfu_debug_level >= MFU_LOG_VERBOSE && mfu_rank == 0) {
        MFU_LOG(MFU_LOG_INFO, "Copying file data");
    }

    int ranks;
    MPI_Comm_size(MPI_COMM_WORLD, &ranks);

    /* get number of items on each process */
    int* rank_counts = (int*) MFU_MALLOC(ranks * sizeof(int));
    int listsize_int = (int) listsize;
    MPI_Allgather(&listsize_int, 1, MPI_INT, rank_counts, 1, MPI_INT, MPI_COMM_WORLD);

    /* get list of item offsets across ranks,
     * this will be used to compute global index of item
     * given a owner rank and list index of the item on its owner rank */
    int* rank_disps = (int*) MFU_MALLOC(ranks * sizeof(int));
    int item_offset = (int) mfu_flist_global_offset(flist);
    MPI_Allgather(&item_offset, 1, MPI_INT, rank_disps, 1, MPI_INT, MPI_COMM_WORLD);

    /* get byte offset in archive for start of data for every item */
    uint64_t* all_offsets = (uint64_t*) MFU_MALLOC(total_items * sizeof(uint64_t));
    MPI_Allgatherv(
        data_offsets, listsize_int, MPI_UINT64_T,
        all_offsets, rank_counts, rank_disps, MPI_UINT64_T,
        MPI_COMM_WORLD);

    /* chunk flist */
    mfu_file_chunk* data_chunks = mfu_file_chunk_list_alloc(flist, opts->chunk_size);

    /* initialize counters to track number of bytes and items extracted */
    reduce_buf[REDUCE_BYTES] = 0;

    /* start progress messages while setting metadata */
    mfu_progress* create_prog = mfu_progress_start(mfu_progress_timeout, 1, MPI_COMM_WORLD, create_progress_fn);

    /* iterate over items and copy data for each one */
    mfu_file_chunk* p = data_chunks;
    while (p != NULL) {
        /* compute global index of item */
        int owner_rank = p->rank_of_owner;
        uint64_t global_idx = rank_disps[owner_rank] + p->index_of_owner;
        uint64_t data_offset = all_offsets[global_idx];

        /* open the source file for reading */
        const char* in_name = p->name;
        int in_fd = mfu_open(p->name, O_RDONLY);
        if (in_fd < 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to open source file '%s' errno=%d %s",
                in_name, errno, strerror(errno));
            DTAR_err = 1;
            break;
        }

        /* copy data from source files to archive file */
        uint64_t bytes_copied = 0;
        uint64_t length = p->length;
        while (bytes_copied < length) {
            /* compute number of bytes to read in this step */
            size_t bytes_to_read = bufsize;
            uint64_t remainder = length - bytes_copied;
            if (remainder < (uint64_t) bytes_to_read) {
                bytes_to_read = (size_t) remainder;
            }

            /* read data from source file */
            off_t pos_read = (off_t)p->offset + (off_t)bytes_copied;
            ssize_t nread = mfu_pread(p->name, in_fd, buf, bytes_to_read, pos_read);
            if (nread < 0) {
                MFU_LOG(MFU_LOG_ERR, "Failed to read source file '%s' errno=%d %s",
                    in_name, errno, strerror(errno));
                DTAR_err = 1;
                break;
            }

            /* write data to the archive file */
            off_t pos_write = (off_t)data_offset + pos_read;
            ssize_t nwrite = mfu_pwrite(filename, fd, buf, nread, pos_write);
            if (nwrite < 0) {
                MFU_LOG(MFU_LOG_ERR, "Failed to write to archive file '%s' errno=%d %s",
                    filename, errno, strerror(errno));
                DTAR_err = 1;
                break;
            }

            /* update number of bytes written */
            bytes_copied += nwrite;

            /* update number of items we have completed for progress messages */
            reduce_buf[REDUCE_BYTES] += nwrite;
            mfu_progress_update(reduce_buf, create_prog);
        }

        int close_rc = mfu_close(p->name, in_fd);
        if (close_rc == -1) {
           /* worth reporting, don't consider this a fatal error */
           MFU_LOG(MFU_LOG_ERR, "Failed to close source file '%s' errno=%d %s",
               in_name, errno, strerror(errno));
        }

        /* advance to next file segment in our list */
        p = p->next;
    }

    /* finalize progress messages */
    mfu_progress_complete(reduce_buf, &create_prog);

    /* free chunk list */
    mfu_file_chunk_list_free(&data_chunks);

    /* rank 0 finalizes the archive by writing two 512-byte blocks of NUL
     * (according to tar file format) */
    if (mfu_rank == 0) {
        /* write two blocks of 512 bytes of 0 */
        char buf[1024] = {0};
        size_t bufsize = sizeof(buf);
        ssize_t pwrite_rc = mfu_pwrite(filename, fd, buf, bufsize, archive_size);
        if (pwrite_rc != bufsize) {
            MFU_LOG(MFU_LOG_ERR, "Failed to write to archive '%s' at offset %llu errno=%d %s",
                filename, archive_size, errno, strerror(errno));
            DTAR_err = 1;
        }

        /* include final NULL blocks in our stats */
        archive_size += bufsize;
    }

//    lock_rc = llapi_group_unlock(fd, 23);

    /* close archive file */
    int close_rc = mfu_close(filename, fd);
    if (close_rc == -1) {
        MFU_LOG(MFU_LOG_ERR, "Failed to close archive '%s' errno=%d %s",
            filename, errno, strerror(errno));
        DTAR_err = 1;
    }

    /* determine whether everyone succeeded in writing their part */
    int write_success = mfu_alltrue(DTAR_err == 0, MPI_COMM_WORLD);
    if (! write_success) {
        rc = MFU_FAILURE;
    }

    /* wait for all ranks to finish */
    MPI_Barrier(MPI_COMM_WORLD);

    /* free sorted list */
    mfu_free(&flist);

    /* stop overall time */
    time_t time_ended;
    time(&time_ended);
    double wtime_ended = MPI_Wtime();

    /* print stats */
    double secs = wtime_ended - wtime_started;
    if (mfu_rank == 0) {
        char starttime_str[256];
        struct tm* localstart = localtime(&time_started);
        strftime(starttime_str, 256, "%b-%d-%Y, %H:%M:%S", localstart);

        char endtime_str[256];
        struct tm* localend = localtime(&time_ended);
        strftime(endtime_str, 256, "%b-%d-%Y, %H:%M:%S", localend);

        /* convert size to units */
        double size_tmp;
        const char* size_units;
        mfu_format_bytes(archive_size, &size_tmp, &size_units);

        /* convert bandwidth to unit */
        double agg_rate_tmp;
        double agg_rate = (double)archive_size / secs;
        const char* agg_rate_units;
        mfu_format_bw(agg_rate, &agg_rate_tmp, &agg_rate_units);

        MFU_LOG(MFU_LOG_INFO, "Started:   %s", starttime_str);
        MFU_LOG(MFU_LOG_INFO, "Completed: %s", endtime_str);
        MFU_LOG(MFU_LOG_INFO, "Seconds: %.3lf", secs);
        MFU_LOG(MFU_LOG_INFO, "Archive size: %.3lf %s", size_tmp, size_units);
        MFU_LOG(MFU_LOG_INFO, "Rate: %.3lf %s " \
                "(%.3" PRIu64 " bytes in %.3lf seconds)", \
                agg_rate_tmp, agg_rate_units, archive_size, secs);
    }

    /* clean up */
    mfu_free(&all_offsets);
    mfu_free(&rank_disps);
    mfu_free(&rank_counts);
    mfu_free(&header_buf);
    mfu_free(&buf);
    mfu_free(&data_offsets);
    mfu_free(&offsets);
    mfu_free(&entry_sizes);
    mfu_free(&header_sizes);

    return rc;
}

int mfu_flist_archive_create(
    mfu_flist flist,
    const char* filename,
    int numpaths,
    const mfu_param_path* paths,
    const mfu_param_path* cwdpath,
    mfu_archive_opts_t* opts)
{
    /* allow override algorithm choice via environment variable */
    char varname[] = "MFU_FLIST_ARCHIVE_CREATE";
    const char* value = getenv(varname);
    if (value != NULL) {
        if (strcmp(value, "LIBCIRCLE") == 0) {
            if (mfu_rank == 0) {
                MFU_LOG(MFU_LOG_INFO, "%s: LIBCIRCLE", varname);
            }
            opts->create_libcircle = 1;
        } else if (strcmp(value, "CHUNK") == 0) {
            if (mfu_rank == 0) {
                MFU_LOG(MFU_LOG_INFO, "%s: CHUNK", varname);
            }
            opts->create_libcircle = 0;
        } else {
            if (mfu_rank == 0) {
                MFU_LOG(MFU_LOG_ERR, "%s: Unknown value: %s", varname, value);
            }
        }
    }

    int rc;
    if (opts->create_libcircle) {
        rc = mfu_flist_archive_create_libcircle(flist, filename, numpaths, paths, cwdpath, opts);
    } else {
        rc = mfu_flist_archive_create_chunk(flist, filename, numpaths, paths, cwdpath, opts);
    }
    return rc;
}

/* copy data from read archive (ar) to write archive (aw),
 * this is used when assigning a full entry to a process
 * copy from one archive to another */
static int copy_data(struct archive* ar, struct archive* aw)
{
    int rc = MFU_SUCCESS;

    while (1) {
        /* extract a block of data from the archive */
        const void* buff;
        size_t size;
        off_t offset;
        int r = archive_read_data_block(ar, &buff, &size, &offset);
        if (r == ARCHIVE_EOF) {
            /* hit end of data for entry */
            break;
        }
        if (r != ARCHIVE_OK) {
            /* read error */
            MFU_LOG(MFU_LOG_ERR, "%s", archive_error_string(ar));
            rc = MFU_FAILURE;
            break;
        }

        /* write that block of data to the item on disk */
        r = archive_write_data_block(aw, buff, size, offset);
        if (r != ARCHIVE_OK) {
            /* write error */
            MFU_LOG(MFU_LOG_ERR, "%s", archive_error_string(ar));
            rc = MFU_FAILURE;
            break;
        }

        /* track number of bytes written so far */
        reduce_buf[REDUCE_BYTES] += (uint64_t) size;

        /* update number of items we have completed for progress messages */
        mfu_progress_update(reduce_buf, extract_prog);
    }

    return rc;
}

/* Given a path to an archive, scan archive to determine number
 * of entries and the byte offset to each one.
 * Returns MFU_SUCCESS if successful, MFU_FAILURE otherwise.
 * Records number of entries in out_count and returns a newly
 * allocated list of offset values in out_offsets if successful. */
static int index_entries(
    const char* filename,   /* name of archive to scan */
    uint64_t* out_count,    /* number of entries found in archive (set if successful) */
    uint64_t** out_offsets) /* list of byte offsets for each entry (if successful), caller must free */
{
    int r;

    /* assume we'll succeed */
    int rc = MFU_SUCCESS;

    /* indicate to user what phase we're in */
    if (mfu_rank == 0) {
        MFU_LOG(MFU_LOG_INFO, "Indexing archive");
    }

    /* have rank 0 scan archive to count up number of entries */
    uint64_t count = 0;
    uint64_t* offsets = NULL;
    if (mfu_rank == 0) {
        /* get file size so we can print percent progress as we scan */
        uint64_t filesize = 0;
        struct stat st;
        int stat_rc = mfu_stat(filename, &st);
        if (stat_rc == 0) {
            /* stat succeeded, get the file size */
            filesize = st.st_size;
        } else {
            /* failed to stat the archive file,
             * we'll keep going, but progress messages will be disabled */
            MFU_LOG(MFU_LOG_ERR, "Failed to stat archive %s (errno=%d %s)",
                filename, errno, strerror(errno)
            );
        }

        /* initiate archive object for reading */
        struct archive* a = archive_read_new();

        /* cannot index an archive that is compressed, only a pure tar format */
//        archive_read_support_filter_bzip2(a);
//        archive_read_support_filter_gzip(a);
//        archive_read_support_filter_compress(a);
        archive_read_support_format_tar(a);

        /* read from stdin if not given a file? */
        if (filename != NULL && strcmp(filename, "-") == 0) {
            filename = NULL;
        }
    
        /* just scanning through headers, so we use a smaller blocksize */
        r = archive_read_open_filename(a, filename, 10240);
        if (r != ARCHIVE_OK) {
            /* failed to read archive, either file does not exist
             * or it may be a format we don't support */
            rc = MFU_FAILURE;
        }

#if 0
        /* TODO: scan for compression filters and bail out if we find any */
        // see archive_filter_count/code calls to iterate over filters
        int filter_count = archive_filter_count(a);
        int i;
        for (i = 0; i < filter_count; i++) {
            uint64_t filter_bytes = archive_filter_bytes(a, i);
            int filter_code = archive_filter_code(a, i);
            const char* filter_name = archive_filter_name(a, i);
            printf("bytes=%llu code=%d name=%s\n", filter_bytes, filter_code, filter_name);
        }
#endif

        /* start timer for progress messages */
        double start = MPI_Wtime();
        double last = start;
    
        /* read entries one by one until we hit the EOF */
        size_t maxcount = 1024;
        offsets = (uint64_t*) malloc(maxcount * sizeof(uint64_t));
        while (rc == MFU_SUCCESS) {
            /* increase our buffer capacity if needed */
            if (count >= maxcount) {
                /* ran out of slots, double capacity and allocate again */
                maxcount *= 2;
                offsets = realloc(offsets, maxcount * sizeof(uint64_t));
            }

            /* read header for the current entry */
            struct archive_entry* entry;
            r = archive_read_next_header(a, &entry);
            if (r == ARCHIVE_EOF) {
                /* found the end of the archive, we're done */
                break;
            }
            if (r != ARCHIVE_OK) {
                MFU_LOG(MFU_LOG_ERR, "Failed to read entry %s",
                    archive_error_string(a)
                );
                rc = MFU_FAILURE;
                break;
            }

            /* get offset of this header */
            uint64_t offset = (uint64_t) archive_read_header_position(a);
            offsets[count] = offset;

            /* increment our count and move on to next entry */
            count++;

            /* print progress message if needed */
            double now = MPI_Wtime();
            if (mfu_progress_timeout > 0 &&
                (now - last) > mfu_progress_timeout &&
                filesize > 0)
            {
                /* compute percent progress and estimated time remaining */
                double percent = (double)offset * 100.0 / (double)filesize;
                double secs = now - start;
                double secs_remaining = 0.0;
                if (percent > 0.0) {
                    secs_remaining = (double)(100.0 - percent) * secs / percent;
                }
                MFU_LOG(MFU_LOG_INFO, "Indexed %llu items in %.3lf secs (%.0f%%) %.0f secs left ...",
                    count, secs, percent, secs_remaining
                );
                last = now;
            }
        }

        /* print a final progress message if we may have printed any */
        double now = MPI_Wtime();
        double secs = now - start;
        if (rc == MFU_SUCCESS &&
            mfu_progress_timeout > 0 &&
            secs > mfu_progress_timeout)
        {
            MFU_LOG(MFU_LOG_INFO, "Indexed %llu items in %.3lf secs (100%%) done",
                count, secs
            );
        }

        /* close our read archive to clean up */
        archive_read_close(a);
        archive_read_free(a);
    }
   
    /* broadcast whether rank 0 actually read archive successfully */
    MPI_Bcast(&rc, 1, MPI_INT, 0, MPI_COMM_WORLD);

    /* bail out if rank 0 failed to index the archive */
    if (rc != MFU_SUCCESS) {
        mfu_free(&offsets);
        return rc;
    }

    /* get count of items from rank 0 */
    MPI_Bcast(&count, 1, MPI_UINT64_T, 0, MPI_COMM_WORLD);

    /* allocate memory to holding incoming offset values */
    if (mfu_rank != 0) {
        offsets = (uint64_t*) MFU_MALLOC(count * sizeof(uint64_t));
    }

    /* get offset values from rank 0 */
    MPI_Bcast(offsets, count, MPI_UINT64_T, 0, MPI_COMM_WORLD);

    /* return count and list of offsets */
    *out_count   = count;
    *out_offsets = offsets;

    return rc; 
}

/* given an entry data structure read from the archive,
 * create a corresponding item in the flist */
static void insert_entry_into_flist(
    struct archive_entry* entry, /* entry to be inserted */
    mfu_flist flist,             /* flist in which to insert item */
    const mfu_path* prefix)      /* prepend prefix to entry path to get absolute path for flist */
{
    /* allocate a new item in our list, and get its index */
    uint64_t idx = mfu_flist_file_create(flist);

    /* get name of archive entry, this is likely a relative path */
    const char* name = archive_entry_pathname(entry);

    /* name in the archive is relative,
     * but paths in flist are absolute (typically),
     * prepend given prefix and reduce resulting path */
    mfu_path* path = mfu_path_from_str(name);
    mfu_path_prepend(path, prefix);
    mfu_path_reduce(path);
    const char* name2 = mfu_path_strdup(path);
    mfu_flist_file_set_name(flist, idx, name2);
    mfu_free(&name2);
    mfu_path_delete(&path);

    /* get mode of entry, and deduce mfu type */
    mode_t mode = archive_entry_mode(entry);
    mfu_filetype type = mfu_flist_mode_to_filetype(mode);
    mfu_flist_file_set_type(flist, idx, type);

    mfu_flist_file_set_mode(flist, idx, mode);

    uint64_t uid = archive_entry_uid(entry);
    mfu_flist_file_set_uid(flist, idx, uid);

    uint64_t gid = archive_entry_gid(entry);
    mfu_flist_file_set_gid(flist, idx, gid);

    uint64_t atime = archive_entry_atime(entry);
    mfu_flist_file_set_atime(flist, idx, atime);

    uint64_t atime_nsec = archive_entry_atime_nsec(entry);
    mfu_flist_file_set_atime_nsec(flist, idx, atime_nsec);

    uint64_t mtime = archive_entry_mtime(entry);
    mfu_flist_file_set_mtime(flist, idx, mtime);

    uint64_t mtime_nsec = archive_entry_mtime_nsec(entry);
    mfu_flist_file_set_mtime_nsec(flist, idx, mtime_nsec);

    uint64_t ctime = archive_entry_ctime(entry);
    mfu_flist_file_set_ctime(flist, idx, ctime);

    uint64_t ctime_nsec = archive_entry_ctime_nsec(entry);
    mfu_flist_file_set_ctime_nsec(flist, idx, ctime_nsec);

    uint64_t size = archive_entry_size(entry);
    mfu_flist_file_set_size(flist, idx, size);
}

/* Given an archvive file, build file list of corresponding items,
 * given a list of offsets to all items */
static int extract_flist_offsets(
    const char* filename,          /* name of archive file */
    const mfu_param_path* cwdpath, /* path to prepend to any relative path in archive */
    uint64_t entries,              /* total number of entries in archive */
    uint64_t entry_start,          /* starting offset this process should handle */
    uint64_t entry_count,          /* number of consecutive entries this process should read */
    uint64_t* offsets,             /* offset to header of each entry */
    uint64_t* data_offsets,        /* returns offset to start of data for each entry */
    mfu_flist flist)               /* file list in which to insert items */
{
    int r;

    /* assume we'll succeed */
    int rc = MFU_SUCCESS;

    /* indicate to user what phase we're in */
    if (mfu_rank == 0) {
        MFU_LOG(MFU_LOG_INFO, "Extracting metadata");
    }

    /* prepare list for metadata details */
    mfu_flist_set_detail(flist, 1);

    /* open archive file for readhing */
    int fd = mfu_open(filename, O_RDONLY);
    if (fd < 0) {
        MFU_LOG(MFU_LOG_ERR, "Failed to open archive: '%s' (errno=%d %s)",
            filename, errno, strerror(errno)
        );
        rc = MFU_FAILURE;
    }

    /* bail out with an error if anyone failed to open the archive */
    if (! mfu_alltrue(rc == MFU_SUCCESS, MPI_COMM_WORLD)) {
        if (fd >= 0) {
            mfu_close(filename, fd);
        }
        return MFU_FAILURE;
    }

    /* get current working directory to prepend to
     * each entry to construct full path */
    mfu_path* cwd = mfu_path_from_str(cwdpath->path);

    /* allocate buffer to hold data offset for each of our items */
    uint64_t* doffsets = (uint64_t*) MFU_MALLOC(entry_count * sizeof(uint64_t));

    /* iterate over each entry we're responsible for */
    uint64_t count = 0;
    while (count < entry_count) {
        /* compute offset and seek to this entry */
        uint64_t idx = entry_start + count;
        off_t offset = (off_t) offsets[idx];
        off_t pos = mfu_lseek(filename, fd, offset, SEEK_SET);
        if (pos == (off_t)-1) {
            MFU_LOG(MFU_LOG_ERR, "Failed to lseek to offset %llu in %s (errno=%d %s)",
                offset, filename, errno, strerror(errno)
            );
            rc = MFU_FAILURE;
            break;
        }

        /* initiate archive object for reading */
        struct archive* a = archive_read_new();

        /* when using an index, we can assume the archive is not compressed */
//        archive_read_support_filter_bzip2(a);
//        archive_read_support_filter_gzip(a);
//        archive_read_support_filter_compress(a);
        archive_read_support_format_tar(a);

        /* can use a small block size since we're just reading header info */
        r = archive_read_open_fd(a, fd, 10240);
        if (r != ARCHIVE_OK) {
            MFU_LOG(MFU_LOG_ERR, "Failed to open archive to extract entry %llu at offset %llu %s",
                idx, offset, archive_error_string(a)
            );
            archive_read_free(a);
            rc = MFU_FAILURE;
            break;
        }

        /* read entry header from archive */
        struct archive_entry* entry;
        r = archive_read_next_header(a, &entry);
        if (r == ARCHIVE_EOF) {
            MFU_LOG(MFU_LOG_ERR, "Unexpected end of archive, read %llu of %llu entries",
                count, entry_count
            );
            archive_read_close(a);
            archive_read_free(a);
            rc = MFU_FAILURE;
            break;
        }
        if (r != ARCHIVE_OK) {
            MFU_LOG(MFU_LOG_ERR, "Failed to extract entry %llu at offset %llu %s",
                idx, offset, archive_error_string(a)
            );
            archive_read_close(a);
            archive_read_free(a);
            rc = MFU_FAILURE;
            break;
        }

        /* read the entry, create a corresponding flist entry for it */
        insert_entry_into_flist(entry, flist, cwd);

        /* get byte position, which would be start of data for a regular file */
        uint64_t header_size = (uint64_t) archive_filter_bytes(a, -1);
        doffsets[count] = offset + header_size;

        /* close out the read archive, to be sure it doesn't have memory */
        r = archive_read_close(a);
        if (r != ARCHIVE_OK) {
            MFU_LOG(MFU_LOG_ERR, "Failed to close archive after extracting entry %llu at offset %llu %s",
                idx, offset, archive_error_string(a)
            );
            archive_read_free(a);
            rc = MFU_FAILURE;
            break;
        }

        /* release read archive */
        r = archive_read_free(a);
        if (r != ARCHIVE_OK) {
            MFU_LOG(MFU_LOG_ERR, "Failed to free archive after extracting entry %llu at offset %llu %s",
                idx, offset, archive_error_string(a)
            );
            rc = MFU_FAILURE;
            break;
        }

        /* advance to next entry */
        count++;
    }

    mfu_flist_summarize(flist);

    /* gather data offsets for all entries to all ranks */
    int ranks;
    MPI_Comm_size(MPI_COMM_WORLD, &ranks);

    /* get number of items on each process */
    int* rank_counts = (int*) MFU_MALLOC(ranks * sizeof(int));
    int listsize_int = (int) mfu_flist_size(flist);
    MPI_Allgather(&listsize_int, 1, MPI_INT, rank_counts, 1, MPI_INT, MPI_COMM_WORLD);

    /* get list of item offsets across ranks,
     * this will be used to compute global index of item
     * given a owner rank and list index of the item on its owner rank */
    int* rank_disps = (int*) MFU_MALLOC(ranks * sizeof(int));
    int item_offset = (int) mfu_flist_global_offset(flist);
    MPI_Allgather(&item_offset, 1, MPI_INT, rank_disps, 1, MPI_INT, MPI_COMM_WORLD);

    /* get byte offset in archive for start of data for every item */
    uint64_t total_items = mfu_flist_global_size(flist);
    MPI_Allgatherv(
        doffsets, listsize_int, MPI_UINT64_T,
        data_offsets, rank_counts, rank_disps, MPI_UINT64_T,
        MPI_COMM_WORLD);

    mfu_free(&rank_counts);
    mfu_free(&rank_disps);

    mfu_path_delete(&cwd);

    mfu_close(filename, fd);

    /* check that all ranks succeeded */
    if (! mfu_alltrue(rc == MFU_SUCCESS, MPI_COMM_WORLD)) {
        rc = MFU_FAILURE;
    }

    return rc;
}

/* Given an archive file, build file list of corresponding items.
 * All processes scan the archive and extract items in a round-robin manner. */
static int extract_flist(
    const char* filename,          /* name of archive file */
    const mfu_param_path* cwdpath, /* path to prepend to relative path of each entry */
    mfu_flist flist)               /* flist list to insert items into */
{
    /* assume we'll succeed */
    int rc = MFU_SUCCESS;

    /* prepare list for metadata details */
    mfu_flist_set_detail(flist, 1);

    /* indicate to user what phase we're in */
    if (mfu_rank == 0) {
        MFU_LOG(MFU_LOG_INFO, "Extracting metadata");
    }

    /* initiate archive object for reading */
    struct archive* a = archive_read_new();

    /* we want all the format supports */
    archive_read_support_filter_bzip2(a);
    archive_read_support_filter_gzip(a);
    archive_read_support_filter_compress(a);
    archive_read_support_format_tar(a);

    if (filename != NULL && strcmp(filename, "-") == 0) {
        filename = NULL;
    }

    /* blocksize set to 1024K */
    int r = archive_read_open_filename(a, filename, 10240);
    if (r != ARCHIVE_OK) {
        MFU_LOG(MFU_LOG_ERR, "%s", archive_error_string(a));
        exit(r);
    }

    /* get current working directory */
    mfu_path* cwd = mfu_path_from_str(cwdpath->path);

    /* get number of ranks in our communicator */
    int ranks;
    MPI_Comm_size(MPI_COMM_WORLD, &ranks);

    /* Read through archive and extract items to our list
     * in a round-robin fashion.  This means different procs
     * will be reading interleaved entries from the archive,
     * but it enables all procs to get to work faster rather
     * than have some end early and others scan to the very
     * end (load imbalance). */
    uint64_t count = 0;
    while (1) {
        /* read next item from archive */
        struct archive_entry* entry;
        r = archive_read_next_header(a, &entry);
        if (r == ARCHIVE_EOF) {
            /* hit end of the archive */
            break;
        }
        if (r != ARCHIVE_OK) {
            MFU_LOG(MFU_LOG_ERR, "%s", archive_error_string(a));
            exit(r);
        }

        /* extract items round-robin across ranks */
        if (count % ranks == mfu_rank) {
            insert_entry_into_flist(entry, flist, cwd);
        }

        /* advance to the next item */
        count++;
    }

    /* close out our list */
    mfu_flist_summarize(flist);

    mfu_path_delete(&cwd);

    archive_read_close(a);
    archive_read_free(a);

    /* figure out whether anyone failed */
    if (! mfu_alltrue(rc == MFU_SUCCESS, MPI_COMM_WORLD)) {
        rc = MFU_FAILURE;
    }

    return rc;
}

/* progress message to print running count of bytes and items processed
 * while extracting items from the archive */
static void extract2_progress_fn(const uint64_t* vals, int count, int complete, int ranks, double secs)
{
    /* compute average rate */
    double byte_rate = 0.0;
    double item_rate = 0.0;
    if (secs > 0) {
        byte_rate = (double)vals[REDUCE_BYTES] / secs;
        item_rate = (double)vals[REDUCE_ITEMS] / secs;
    }

    /* format number of bytes for printing */
    double bytes_val = 0.0;
    const char* bytes_units = NULL;
    mfu_format_bytes(vals[REDUCE_BYTES], &bytes_val, &bytes_units);

    /* format bandwidth for printing */
    double bw_val = 0.0;
    const char* bw_units = NULL;
    mfu_format_bw(byte_rate, &bw_val, &bw_units);

    /* compute percentage of bytes extracted */
    double percent = 0.0;
    if (DTAR_total_bytes > 0) {
        percent = (double)vals[REDUCE_BYTES] * 100.0 / (double)DTAR_total_bytes;
    }

    /* estimate seconds remaining */
    double secs_remaining = 0.0;
    if (byte_rate > 0.0) {
        secs_remaining = (double)(DTAR_total_bytes - vals[REDUCE_BYTES]) / byte_rate;
    }

    if (complete < ranks) {
        MFU_LOG(MFU_LOG_INFO,
            "Extracted %llu items and %.3lf %s (%.0f\%) in %.3lf secs (%.3lf items/sec, %.3lf %s) %.0f secs left ...",
            vals[REDUCE_ITEMS], bytes_val, bytes_units, percent, secs, item_rate, bw_val, bw_units, secs_remaining
        );
    } else {
        MFU_LOG(MFU_LOG_INFO,
            "Extracted %llu items and %.3lf %s (%.0f\%) in %.3lf secs (%.3lf items/sec, %.3lf %s) done",
            vals[REDUCE_ITEMS], bytes_val, bytes_units, percent, secs, item_rate, bw_val, bw_units
        );
    }
}

/* progress message to print sum of bytes while extracting items from the archive */
static void extract1_progress_fn(const uint64_t* vals, int count, int complete, int ranks, double secs)
{
    /* compute average rate */
    double byte_rate = 0.0;
    if (secs > 0) {
        byte_rate = (double)vals[REDUCE_BYTES] / secs;
    }

    /* format number of bytes for printing */
    double bytes_val = 0.0;
    const char* bytes_units = NULL;
    mfu_format_bytes(vals[REDUCE_BYTES], &bytes_val, &bytes_units);

    /* format bandwidth for printing */
    double bw_val = 0.0;
    const char* bw_units = NULL;
    mfu_format_bw(byte_rate, &bw_val, &bw_units);

    /* compute percentage of bytes extracted */
    double percent = 0.0;
    if (DTAR_total_bytes > 0) {
        percent = (double)vals[REDUCE_BYTES] * 100.0 / (double)DTAR_total_bytes;
    }

    /* estimate seconds remaining */
    double secs_remaining = 0.0;
    if (byte_rate > 0.0) {
        secs_remaining = (double)(DTAR_total_bytes - vals[REDUCE_BYTES]) / byte_rate;
    }

    if (complete < ranks) {
        MFU_LOG(MFU_LOG_INFO,
            "Extracted %.3lf %s (%.0f\%) in %.3lf secs (%.3lf %s) %.0f secs left ...",
            bytes_val, bytes_units, percent, secs, bw_val, bw_units, secs_remaining
        );
    } else {
        MFU_LOG(MFU_LOG_INFO,
            "Extracted %.3lf %s (%.0f\%) in %.3lf secs (%.3lf %s) done",
            bytes_val, bytes_units, percent, secs, bw_val, bw_units
        );
    }
}

/* Extract items from a given archive file, given the offset of each entry in the archive.
 * This uses libarchive to actually read data from the archive and write to an item on disk. */
static int extract_files_offsets(
    const char* filename,     /* name of archive file */
    int flags,                /* flags passed to archive_write_disk_set_options */
    uint64_t entries,         /* number of total entries in offsets array */
    uint64_t entry_start,     /* starting offset in offsets we're responsible for */
    uint64_t entry_count,     /* number of entries we're responsible for */
    uint64_t* offsets,        /* offset to each entry in the archive */
    mfu_flist flist,          /* file list corresponding to our subset of entries */
    mfu_archive_opts_t* opts) /* options to configure extract operation */
{
    /* assume we'll succeed */
    int rc = MFU_SUCCESS;

    /* indicate to user what phase we're in */
    if (mfu_rank == 0) {
        MFU_LOG(MFU_LOG_INFO, "Extracting items");
    }

    /* intitialize counters to track number of bytes and items extracted */
    reduce_buf[REDUCE_BYTES] = 0;
    reduce_buf[REDUCE_ITEMS] = 0;

    /* start progress messages while setting metadata */
    extract_prog = mfu_progress_start(mfu_progress_timeout, 2, MPI_COMM_WORLD, extract2_progress_fn);

    /* open the archive file for reading */
    int fd = mfu_open(filename, O_RDONLY);
    if (fd < 0) {
        MFU_LOG(MFU_LOG_ERR, "Failed to open archive: '%s' errno=%d %s",
            filename, errno, strerror(errno)
        );
        rc = MFU_FAILURE;
    }

    /* check that everyone opened the archive successfully */
    if (! mfu_alltrue(rc == MFU_SUCCESS, MPI_COMM_WORLD)) {
        /* someone failed, close the file if we opened it and return */
        if (fd >= 0) {
            mfu_close(filename, fd);
        }
        return MFU_FAILURE;
    }

    /* initiate object for writing items out to disk */
    struct archive* ext = archive_write_disk_new();
    int r = archive_write_disk_set_options(ext, flags);
    if (r != ARCHIVE_OK) {
        MFU_LOG(MFU_LOG_ERR, "Failed to set options on write object %s",
            archive_error_string(ext)
        );
        rc = MFU_FAILURE;
    }

    /* use system calls to lookup uname/gname (follows POSIX pax) */
    r = archive_write_disk_set_standard_lookup(ext);
    if (r != ARCHIVE_OK) {
        MFU_LOG(MFU_LOG_ERR, "Failed to set standard uname/gname lookup on write object %s",
            archive_error_string(ext)
        );
        rc = MFU_FAILURE;
    }

    /* iterate over and extract each item we're responsible for */
    uint64_t count = 0;
    while (count < entry_count && rc == MFU_SUCCESS) {
        /* seek to start of the entry in the archive file */
        uint64_t idx = entry_start + count;
        off_t offset = (off_t) offsets[idx];
        off_t pos = mfu_lseek(filename, fd, offset, SEEK_SET);
        if (pos == (off_t)-1) {
            MFU_LOG(MFU_LOG_ERR, "Failed to seek to offset %llu in open archive: '%s' errno=%d %s",
                offset, filename, errno, strerror(errno)
            );
            rc = MFU_FAILURE;
            break;
        }

        /* initiate archive object for reading,
         * we do this new each time to be sure that state is
         * not left over from the previous item */
        struct archive* a = archive_read_new();

        /* when using offsets, we assume there is no compression */
//        archive_read_support_filter_bzip2(a);
//        archive_read_support_filter_gzip(a);
//        archive_read_support_filter_compress(a);
        archive_read_support_format_tar(a);

        /* we can use a large blocksize for reading,
         * since we'll read headers and data in a contguous
         * region of the file */
        r = archive_read_open_fd(a, fd, opts->buf_size);
        if (r != ARCHIVE_OK) {
            MFU_LOG(MFU_LOG_ERR, "opening archive to extract entry %llu at offset %llu %s",
                idx, offset, archive_error_string(a)
            );
            archive_read_free(a);
            rc = MFU_FAILURE;
            break;
        }

        /* read the entry header for this item */
        struct archive_entry* entry;
        r = archive_read_next_header(a, &entry);
        if (r == ARCHIVE_EOF) {
            MFU_LOG(MFU_LOG_ERR, "unexpected end of archive, read %llu of %llu items",
                count, entry_count
            );
            archive_read_close(a);
            archive_read_free(a);
            rc = MFU_FAILURE;
            break;
        }
        if (r != ARCHIVE_OK) {
            MFU_LOG(MFU_LOG_ERR, "extracting entry %llu at offset %llu %s",
                idx, offset, archive_error_string(a)
            );
            archive_read_close(a);
            archive_read_free(a);
            rc = MFU_FAILURE;
            break;
        }

        /* got an entry, create corresponding item on disk and
         * then copy data */
        r = archive_write_header(ext, entry);
        if (r != ARCHIVE_OK) {
            MFU_LOG(MFU_LOG_ERR, "writing entry %llu at offset %llu %s",
                idx, offset, archive_error_string(ext)
            );
            archive_read_close(a);
            archive_read_free(a);
            rc = MFU_FAILURE;
            break;
        } else {
            /* extract file data (if item is a file) */
            int tmp_rc = copy_data(a, ext);
            if (tmp_rc != MFU_SUCCESS) {
                rc = tmp_rc;
                archive_read_close(a);
                archive_read_free(a);
                break;
            }
        }

        /* increment our count of items extracted */
        reduce_buf[REDUCE_ITEMS]++;

        /* update number of items we have completed for progress messages */
        mfu_progress_update(reduce_buf, extract_prog);

        /* close out the read archive object */
        r = archive_read_close(a);
        if (r != ARCHIVE_OK) {
            MFU_LOG(MFU_LOG_ERR, "Failed to close read archive %s",
                archive_error_string(a)
            );
            archive_read_free(a);
            rc = MFU_FAILURE;
            break;
        }

        /* free memory allocated in read archive object */
        r = archive_read_free(a);
        if (r != ARCHIVE_OK) {
            MFU_LOG(MFU_LOG_ERR, "Failed to free read archive %s",
                archive_error_string(a)
            );
            rc = MFU_FAILURE;
            break;
        }

        /* advance to our next entry */
        count++;
    }

    /* close out our write archive, this may update timestamps and permissions on items */
    r = archive_write_close(ext);
    if (r != ARCHIVE_OK) {
        MFU_LOG(MFU_LOG_ERR, "Failed to close archive for writing to disk %s",
            archive_error_string(ext)
        );
        rc = MFU_FAILURE;
    }

    /* free off our write archive, this may update timestamps and permissions on items */
    r = archive_write_free(ext);
    if (r != ARCHIVE_OK) {
        MFU_LOG(MFU_LOG_ERR, "Failed to free archive for writing to disk %s",
            archive_error_string(ext)
        );
        rc = MFU_FAILURE;
    }

    /* finalize progress messages */
    mfu_progress_complete(reduce_buf, &extract_prog);

    /* Ensure all ranks have created all items before we close the write archive.
     * libarchive can update timestamps on directories when closing out,
     * so we want to ensure all child items exist first. */
    MPI_Barrier(MPI_COMM_WORLD);

    /* if a directory already exists, libarchive does not currently update
     * its timestamps when closing the write archive */
    MPI_Barrier(MPI_COMM_WORLD);

    /* figure out whether anyone failed */
    if (! mfu_alltrue(rc == MFU_SUCCESS, MPI_COMM_WORLD)) {
        rc = MFU_FAILURE;
    }

    return rc;
}

/* Extract items from a given archive file, given the offset of each entry in the archive.
 * This assumes all items have been created, and it uses mfu chunk lists to directly read
 * data from the archive. */
static int extract_files_offsets_chunk(
    const char* filename,     /* name of archive file */
    int flags,                /* flags - not used in this context */
    uint64_t entries,         /* number of entries in archive */
    uint64_t entry_start,     /* global offset for entry this process should start with */
    uint64_t entry_count,     /* number of consecutive items this process is responsible for */
    uint64_t* data_offsets,   /* offset to start of data for each entry */
    mfu_flist flist,          /* file list whose local elements correspond to items to extract */
    mfu_archive_opts_t* opts) /* options to configure extract operation */
{
    /* assume we'll succeed */
    int rc = MFU_SUCCESS;

    /* indicate to user what phase we're in */
    if (mfu_rank == 0) {
        MFU_LOG(MFU_LOG_INFO, "Extracting items");
    }

    /* open the archive file for reading */
    int fd = mfu_open(filename, O_RDONLY);
    if (fd < 0) {
        MFU_LOG(MFU_LOG_ERR, "Failed to open archive: '%s' errno=%d %s",
            filename, errno, strerror(errno)
        );
        rc = MFU_FAILURE;
    }

    /* check that everyone opened the archive successfully */
    if (! mfu_alltrue(rc == MFU_SUCCESS, MPI_COMM_WORLD)) {
        /* someone failed, close the file if we opened it and return */
        if (fd >= 0) {
            mfu_close(filename, fd);
        }
        return MFU_FAILURE;
    }

    /* get number of ranks in our communicator */
    int ranks;
    MPI_Comm_size(MPI_COMM_WORLD, &ranks);

    /* Get list of item offsets across ranks.
     * This will be used to compute the global index of an item
     * given an owner rank and local index of that item on its owner rank. */
    int* rank_disps = (int*) MFU_MALLOC(ranks * sizeof(int));
    int item_offset = (int) mfu_flist_global_offset(flist);
    MPI_Allgather(&item_offset, 1, MPI_INT, rank_disps, 1, MPI_INT, MPI_COMM_WORLD);

    /* allocate I/O buffer to read/write data */
    size_t bufsize = opts->buf_size;
    void* buf = MFU_MALLOC(bufsize);

    /* split the regular files listed in flist into chunks and distribute
     * those chunks evenly across processes as a linked list */
    mfu_file_chunk* data_chunks = mfu_file_chunk_list_alloc(flist, opts->chunk_size);

    /* initialize counters to track number of bytes and items extracted */
    reduce_buf[REDUCE_BYTES] = 0;

    /* start progress messages while setting metadata,
     * in this case, we can track bytes accurately but not items */
    extract_prog = mfu_progress_start(mfu_progress_timeout, 1, MPI_COMM_WORLD, extract1_progress_fn);
    
    /* iterate over items and copy data for each one */
    mfu_file_chunk* p = data_chunks;
    while (p != NULL && rc == MFU_SUCCESS) {
        /* compute global index of item */
        int owner_rank = p->rank_of_owner;
        uint64_t global_idx = rank_disps[owner_rank] + p->index_of_owner;
        uint64_t data_offset = data_offsets[global_idx];

        /* open the destination file for writing */
        const char* out_name = p->name;
        int out_fd = mfu_open(out_name, O_WRONLY);
        if (out_fd < 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to open destination file '%s' errno=%d %s",
                out_name, errno, strerror(errno));
            rc = MFU_FAILURE;
            break;
        }

        /* copy data from archive file to destination file */
        uint64_t bytes_copied = 0;
        uint64_t length = p->length;
        while (bytes_copied < length) {
            /* compute number of bytes to read in this step */
            size_t bytes_to_read = bufsize;
            uint64_t remainder = length - bytes_copied;
            if (remainder < (uint64_t) bytes_to_read) {
                bytes_to_read = (size_t) remainder;
            }

            /* read data from archive file */
            off_t pos_read = (off_t)data_offset + (off_t)p->offset + (off_t)bytes_copied;
            ssize_t nread = mfu_pread(filename, fd, buf, bytes_to_read, pos_read);
            if (nread < 0) {
                MFU_LOG(MFU_LOG_ERR, "Failed to read archive file '%s' errno=%d %s",
                    filename, errno, strerror(errno));
                rc = MFU_FAILURE;
                break;
            }
            if (nread == 0) {
                MFU_LOG(MFU_LOG_ERR, "Unexpected end of archive file '%s'",
                    filename);
                DTAR_err = 1;
                rc = MFU_FAILURE;
                break;
            }

            /* write data to the file */
            off_t pos_write = (off_t)p->offset + (off_t)bytes_copied;
            ssize_t nwritten = mfu_pwrite(out_name, out_fd, buf, nread, pos_write);
            if (nwritten < 0) {
                MFU_LOG(MFU_LOG_ERR, "Failed to write to destination file '%s' errno=%d %s",
                    out_name, errno, strerror(errno));
                DTAR_err = 1;
                rc = MFU_FAILURE;
                break;
            }

            /* update number of bytes written */
            bytes_copied += nwritten;

            /* update number of items we have completed for progress messages */
            reduce_buf[REDUCE_BYTES] += nwritten;
            mfu_progress_update(reduce_buf, extract_prog);
        }

        /* close the user file being written */
        int close_rc = mfu_close(out_name, out_fd);
        if (close_rc == -1) {
           /* worth reporting, don't consider this a fatal error */
           MFU_LOG(MFU_LOG_ERR, "Failed to close destination file '%s' errno=%d %s",
               out_name, errno, strerror(errno));
        }

        /* advance to next file segment in our list */
        p = p->next;
    }

    /* finalize progress messages */
    mfu_progress_complete(reduce_buf, &extract_prog);

    /* free chunk list */
    mfu_file_chunk_list_free(&data_chunks);

    /* free off memory */
    mfu_free(&rank_disps);
    mfu_free(&buf);

    /* figure out whether anyone failed */
    if (! mfu_alltrue(rc == MFU_SUCCESS, MPI_COMM_WORLD)) {
        rc = MFU_FAILURE;
    }

    return rc;
}

/* Extract items from the specified archive file the hard way.
 * Each process reads the archive from the beginning and extracts
 * items in a round-robin fashion based on its rank number.
 * This permits processing of compressed archives, and those that
 * use global headers. */
static int extract_files(
    const char* filename,    /* name of the archive file */
    int flags,               /* flags to pass to archive_write_disk_set_options */
    uint64_t entries,
    uint64_t entry_start,
    uint64_t entry_count,
    mfu_flist flist,
    mfu_archive_opts_t* opts)
{
    /* assume we'll succeed */
    int rc = MFU_SUCCESS;

    /* indicate to user what phase we're in */
    if (mfu_rank == 0) {
        MFU_LOG(MFU_LOG_INFO, "Extracting items");
    }

    /* initialize counters to track number of bytes extracted */
    reduce_buf[REDUCE_BYTES] = 0;
    reduce_buf[REDUCE_ITEMS] = 0;

    /* start progress messages while setting metadata */
    extract_prog = mfu_progress_start(mfu_progress_timeout, 2, MPI_COMM_WORLD, extract2_progress_fn);

    /* initiate archive object for reading */
    struct archive* a = archive_read_new();

    /* in the general case, we want potential compression
     * schemes in addition to tar format */
    archive_read_support_filter_bzip2(a);
    archive_read_support_filter_gzip(a);
    archive_read_support_filter_compress(a);
    archive_read_support_format_tar(a);

    /* initiate archive object for writing items out to disk */
    struct archive* ext = archive_write_disk_new();
    int r = archive_write_disk_set_options(ext, flags);
    if (r != ARCHIVE_OK) {
        MFU_LOG(MFU_LOG_ERR, "Failed to set options on write object %s",
            archive_error_string(ext)
        );
        rc = MFU_FAILURE;
    }

    /* use system calls to lookup uname/gname (follows POSIX pax) */
    r = archive_write_disk_set_standard_lookup(ext);
    if (r != ARCHIVE_OK) {
        MFU_LOG(MFU_LOG_ERR, "Failed to set standard uname/gname lookup on write object %s",
            archive_error_string(ext)
        );
        rc = MFU_FAILURE;
    }

    /* read from stdin? */
    if (filename != NULL && strcmp(filename, "-") == 0) {
        filename = NULL;
    }

    /* Block size is a bit hard to choose here.  If there are lots of
     * small headers/files, it's best to use a small block size to skip through
     * the list of files quickly without reading excess data.  But if there
     * are a few large files, it would be better to use a large block size
     * to efficiently read data once we get to our file.  Strike a balance
     * of 1MB. */
    r = archive_read_open_filename(a, filename, 1024 * 1024);
    if (r != ARCHIVE_OK) {
        MFU_LOG(MFU_LOG_ERR, "opening archive '%s' %s",
            filename, archive_error_string(a)
        );
        rc = MFU_FAILURE;
    }

    /* get number of ranks in our communicator */
    int ranks;
    MPI_Comm_size(MPI_COMM_WORLD, &ranks);

    /* iterate over all entry from the start of the file,
     * looking to find the range of items it is responsible for */
    uint64_t count = 0;
    while (rc == MFU_SUCCESS) {
        /* read the next entry from the archive */
        struct archive_entry* entry;
        r = archive_read_next_header(a, &entry);
        if (r == ARCHIVE_EOF) {
            break;
        }
        if (r != ARCHIVE_OK) {
            MFU_LOG(MFU_LOG_ERR, "extracting entry %llu %s",
                count, archive_error_string(a)
            );
            rc = MFU_FAILURE;
            break;
        }

        /* write item out to disk if this is one of our assigned items */
        if (count % ranks == mfu_rank) {
            /* create item on disk */
            r = archive_write_header(ext, entry);
            if (r != ARCHIVE_OK) {
                MFU_LOG(MFU_LOG_ERR, "writing entry %llu %s",
                    count, archive_error_string(ext)
                );
                rc = MFU_FAILURE;
                break;
            } else {
                /* extract file data (if item is a file) */
                int tmp_rc = copy_data(a, ext);
                if (tmp_rc != MFU_SUCCESS) {
                    rc = tmp_rc;
                    break;
                }
            }

            /* increment our count of items extracted */
            reduce_buf[REDUCE_ITEMS]++;

            /* update number of items we have completed for progress messages */
            mfu_progress_update(reduce_buf, extract_prog);
        }

        /* advance to next entry in the archive */
        count++;
    }

    /* free off our write archive, this may update timestamps and permissions on items */
    r = archive_write_close(ext);
    if (r != ARCHIVE_OK) {
        MFU_LOG(MFU_LOG_ERR, "Failed to close archive for writing to disk %s",
            archive_error_string(ext)
        );
        rc = MFU_FAILURE;
    }

    /* free off our write archive, this may update timestamps and permissions on items */
    r = archive_write_free(ext);
    if (r != ARCHIVE_OK) {
        MFU_LOG(MFU_LOG_ERR, "Failed to free archive for writing to disk %s",
            archive_error_string(ext)
        );
        rc = MFU_FAILURE;
    }

    /* finalize progress messages */
    mfu_progress_complete(reduce_buf, &extract_prog);

    /* Ensure all ranks have created all items before we close the write archive.
     * libarchive will update timestamps on directories when closing out,
     * so we want to ensure all child items exist at this point. */
    MPI_Barrier(MPI_COMM_WORLD);

    /* close out the read archive object */
    r = archive_read_close(a);
    if (r != ARCHIVE_OK) {
        MFU_LOG(MFU_LOG_ERR, "Failed to close read archive %s",
            archive_error_string(a)
        );
        rc = MFU_FAILURE;
    }

    /* free memory allocated in read archive object */
    r = archive_read_free(a);
    if (r != ARCHIVE_OK) {
        MFU_LOG(MFU_LOG_ERR, "Failed to free read archive %s",
            archive_error_string(a)
        );
        rc = MFU_FAILURE;
    }

    /* if a directory already exists, libarchive does not currently update
     * its timestamps when closing the write archive,
     * update timestamps on directories */
    MPI_Barrier(MPI_COMM_WORLD);

    /* figure out whether anyone failed */
    if (! mfu_alltrue(rc == MFU_SUCCESS, MPI_COMM_WORLD)) {
        rc = MFU_FAILURE;
    }

    return rc;
}

/* iterate through our portion of the given file list,
 * identify symlinks and extract them from archive */
static int extract_symlinks(
    const char* filename,     /* name of archive file */
    mfu_flist flist,          /* file list of items */
    uint64_t* offsets,        /* offset of each item in the archive */
    mfu_archive_opts_t* opts) /* options to configure extraction operation */
{
    int rc = MFU_SUCCESS;

#if 0
    MPI_Barrier(MPI_COMM_WORLD);
    if (mfu_rank == 0) {
        MFU_LOG(MFU_LOG_INFO, "Creating symlinks");
    }
#endif

    /* open the archive file for reading */
    int fd = mfu_open(filename, O_RDONLY);
    if (fd < 0) {
        MFU_LOG(MFU_LOG_ERR, "Failed to open archive: '%s' errno=%d %s",
            filename, errno, strerror(errno)
        );
        rc = MFU_FAILURE;
    }

    /* check that everyone opened the archive successfully */
    if (! mfu_alltrue(rc == MFU_SUCCESS, MPI_COMM_WORLD)) {
        /* someone failed, close the file if we opened it and return */
        if (fd >= 0) {
            mfu_close(filename, fd);
        }
        return MFU_FAILURE;
    }

    /* get global offset of our portion of the list */
    uint64_t global_offset = mfu_flist_global_offset(flist);

    /* iterate over all items in our list and create any symlinks */
    uint64_t idx;
    uint64_t size = mfu_flist_size(flist);
    for (idx = 0; idx < size; idx++) {
        /* skip entries that are not symlinks */
        mfu_filetype type = mfu_flist_file_get_type(flist, idx);
        if (type != MFU_TYPE_LINK) {
            /* not a symlink, go to next item */
            continue;
        }

        /* got a symlink, get its path */
        const char* name = mfu_flist_file_get_name(flist, idx);

        /* seek to start of the corresponding entry in the archive file */
        uint64_t global_idx = global_offset + idx;
        off_t offset = (off_t) offsets[global_idx];
        off_t pos = mfu_lseek(filename, fd, offset, SEEK_SET);
        if (pos == (off_t)-1) {
            MFU_LOG(MFU_LOG_ERR, "Failed to seek to offset %llu in open archive: '%s' errno=%d %s",
                offset, filename, errno, strerror(errno)
            );
            rc = MFU_FAILURE;
            continue;
        }

        /* initiate archive object for reading */
        struct archive* a = archive_read_new();

        /* when using offsets, we assume there is no compression */
//        archive_read_support_filter_bzip2(a);
//        archive_read_support_filter_gzip(a);
//        archive_read_support_filter_compress(a);
        archive_read_support_format_tar(a);

        /* use a small read block size, since we just need the header */
        int r = archive_read_open_fd(a, fd, 10240);
        if (r != ARCHIVE_OK) {
            MFU_LOG(MFU_LOG_ERR, "opening archive to extract symlink `%s' at offset %llu %s",
                name, offset, archive_error_string(a)
            );
            archive_read_free(a);
            rc = MFU_FAILURE;
            continue;
        }

        /* read the entry header for this item */
        struct archive_entry* entry;
        r = archive_read_next_header(a, &entry);
        if (r == ARCHIVE_EOF) {
            MFU_LOG(MFU_LOG_ERR, "Unexpected end of archive while extracting symlink `%s' at offset %llu",
                name, offset
            );
            archive_read_close(a);
            archive_read_free(a);
            rc = MFU_FAILURE;
            continue;
        }
        if (r != ARCHIVE_OK) {
            MFU_LOG(MFU_LOG_ERR, "Extracting symlink '%s' at offset %llu %s",
                name, offset, archive_error_string(a)
            );
            archive_read_close(a);
            archive_read_free(a);
            rc = MFU_FAILURE;
            continue;
        }

        /* get target of the link */
        const char* target = archive_entry_symlink(entry);
        if (target == NULL) {
            MFU_LOG(MFU_LOG_ERR, "Item is not a symlink as expected `%s'",
                name);
            archive_read_close(a);
            archive_read_free(a);
            rc = MFU_FAILURE;
            continue;
        }

        /* create the link on the file system */
        int symlink_rc = mfu_symlink(target, name);
        if (symlink_rc != 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to set symlink `%s' (errno=%d %s)",
                name, errno, strerror(errno));
            rc = MFU_FAILURE;
        }

        /* close out the read archive object */
        r = archive_read_close(a);
        if (r != ARCHIVE_OK) {
            MFU_LOG(MFU_LOG_ERR, "Failed to close read archive %s",
                archive_error_string(a)
            );
            rc = MFU_FAILURE;
        }

        /* free memory allocated in read archive object */
        r = archive_read_free(a);
        if (r != ARCHIVE_OK) {
            MFU_LOG(MFU_LOG_ERR, "Failed to free read archive %s",
                archive_error_string(a)
            );
            rc = MFU_FAILURE;
        }
    }

    /* close the archive file */
    mfu_close(filename, fd);

    /* figure out whether anyone failed */
    if (! mfu_alltrue(rc == MFU_SUCCESS, MPI_COMM_WORLD)) {
        rc = MFU_FAILURE;
    }

    return rc;
}

/* compute total bytes in regular files in flist */
static uint64_t flist_sum_bytes(mfu_flist flist)
{
    /* sum up bytes in our portion of the list */
    uint64_t bytes = 0;
    if (mfu_flist_have_detail(flist)) {
        uint64_t idx = 0;
        uint64_t max = mfu_flist_size(flist);
        for (idx = 0; idx < max; idx++) {
            /* get size of regular files */
            mfu_filetype type = mfu_flist_file_get_type(flist, idx);
            if (type == MFU_TYPE_FILE) {
                uint64_t size = mfu_flist_file_get_size(flist, idx);
                bytes += size;
            }
        }
    }

    /* get total bytes across all ranks */
    uint64_t total_bytes;
    MPI_Allreduce(&bytes, &total_bytes, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
    return total_bytes;
}

/* given an archive file name, extract items into cwdpath according to options */
int mfu_flist_archive_extract(
    const char* filename,          /* name of archive file */
    const mfu_param_path* cwdpath, /* path to prepend to entries in archive to build full path */
    mfu_archive_opts_t* opts)      /* options to configure extract operation */
{
    int rc = MFU_SUCCESS;

    int ranks;
    MPI_Comm_size(MPI_COMM_WORLD, &ranks);

    /* configure flags for libarchive based on archive options */
    int flags = 0;
    flags |= ARCHIVE_EXTRACT_TIME;
//  flags |= ARCHIVE_EXTRACT_OWNER;
    flags |= ARCHIVE_EXTRACT_PERM;

    if (opts->preserve) {
        flags |= ARCHIVE_EXTRACT_XATTR;
        flags |= ARCHIVE_EXTRACT_ACL;
        flags |= ARCHIVE_EXTRACT_FFLAGS;
    }

    /* turn on no overwrite so that directories we create are deleted and then replaced */
    //archive_opts.flags |= ARCHIVE_EXTRACT_NO_OVERWRITE;

    /* configure behavior when creating items (overwrite, lustre striping, etc) */
    mfu_create_opts_t* create_opts = mfu_create_opts_new();

    /* overwrite any existing files by default */
    create_opts->overwrite = true;

    /* Set timestamps and permission bits on extracted items by default.
     * We don't set uid/gid, since the tarball may have encoded uid/gid
     * from another user. */
    create_opts->set_timestamps = true;
    create_opts->set_permissions = true;

    /* TODO: set these based on either auto-detection that CWD is lustre
     * or directives from user */
    //create_opts->lustre_stripe = true;
    //create_opts->lustre_stripe_minsize = 1024ULL * 1024ULL * 1024ULL;

    /* start overall timer */
    time_t time_started;
    time(&time_started);
    double wtime_started = MPI_Wtime();

    /* indicate to user what phase we're in */
    if (mfu_rank == 0) {
        MFU_LOG(MFU_LOG_INFO, "Extracting %s", filename);
    }

    /* get number of entries in archive */
    bool have_offsets = true; /* whether we found offsets for the entries */
    bool have_index   = true; /* whether we have an index file */
    uint64_t entries  = 0;    /* number of entries */
    uint64_t* offsets = NULL; /* byte offset within archive for each entry */
    int ret = read_entry_index(filename, &entries, &offsets);
    if (ret != MFU_SUCCESS) {
        /* don't have an index file */
        have_index = false;

        /* next best option is to scan the archive
         * and see if we can extract entry offsets */
        ret = index_entries(filename, &entries, &offsets);
        if (ret != MFU_SUCCESS) {
            /* failed to get entry offsets,
             * perhaps we have a compressed archive? */
            have_offsets = false;
        }
    }

    /* divide entries among ranks */
    uint64_t entries_per_rank = entries / ranks;
    uint64_t entries_remainder = entries - entries_per_rank * ranks;

    /* compute starting entry and number of entries based on our rank */
    uint64_t entry_start = 0;
    uint64_t entry_count = 0;
    if (mfu_rank < entries_remainder) {
        entry_count = entries_per_rank + 1;
        entry_start = mfu_rank * entry_count;
    } else {
        entry_count = entries_per_rank;
        entry_start = entries_remainder * (entry_count + 1) + (mfu_rank - entries_remainder) * entry_count;
    }

    /* extract metadata for items in archive and construct flist,
     * also get offsets to start of data region for each entry */
    uint64_t* data_offsets = NULL;
    mfu_flist flist = mfu_flist_new();
    if (have_offsets) {
        /* if have offsets, we can likely get the data offsets as well */
        data_offsets = (uint64_t*) MFU_MALLOC(entries * sizeof(uint64_t));

        /* with offsets, we can directly seek to each entry to read its header */
        ret = extract_flist_offsets(filename, cwdpath, entries, entry_start, entry_count, offsets, data_offsets, flist);
    } else {
        /* don't have entry offsets, so scan archive from the start to build flist,
         * assume we can't get data offsets in this case either */
        ret = extract_flist(filename, cwdpath, flist);
    }
    if (ret != MFU_SUCCESS) {
        /* fatal error if we failed to build the flist */
        if (mfu_rank == 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to extract metadata");
        }
        mfu_create_opts_delete(&create_opts);
        mfu_flist_free(&flist);
        mfu_free(&data_offsets);
        mfu_free(&offsets);
        return MFU_FAILURE;
    }

    /* sum up bytes and items in list for tracking progress */
    DTAR_total_bytes = flist_sum_bytes(flist);
    DTAR_total_items = mfu_flist_global_size(flist);

    /* print summary of what's in archive before extracting items */
    mfu_flist_print_summary(flist);

    /* Create all directories in advance to avoid races between a process trying to create
     * a child item and another process responsible for the parent directory.
     * The libarchive code does not remove existing directories,
     * even in normal mode with overwrite. */

    /* indicate to user what phase we're in */
    if (mfu_rank == 0) {
        MFU_LOG(MFU_LOG_INFO, "Creating directories");
    }
    mfu_flist_mkdir(flist, create_opts);

    /* extract files from archive */
    if (have_offsets) {
        /* if we have offsets, we can jump to the start of each entry
         * rather than having to scan from the start of the archive */
        if (opts->extract_libarchive) {
            /* in this case we use libarchve to read entries from the archive
             * and write them to disk, though we can use offsets to seek to
             * the start of each entry */
            ret = extract_files_offsets(filename, flags, entries, entry_start, entry_count, offsets, flist, opts);
        } else {
            /* in this case, we'll use an mfu chunk list to distribute work
             * to processes and read/write from the archive directly */

            /* since more than one process may write to the same file,
             * create the files in advance */
            MPI_Barrier(MPI_COMM_WORLD);
            if (mfu_rank == 0) {
                MFU_LOG(MFU_LOG_INFO, "Creating files");
            }
            mfu_flist_mknod(flist, create_opts);

            /* extract file data from archive */
            ret = extract_files_offsets_chunk(filename, flags, entries, entry_start, entry_count, data_offsets, flist, opts);

            /* create symlinks */
            int tmp_rc = extract_symlinks(filename, flist, offsets, opts);
            if (tmp_rc != MFU_SUCCESS) {
                /* tried but failed to get some symlink, so mark as failure */
                ret = tmp_rc;
            }

            /* set timestamps and permissions on everything */
            MPI_Barrier(MPI_COMM_WORLD);
            if (mfu_rank == 0) {
                MFU_LOG(MFU_LOG_INFO, "Updating timestamps and permissions");
            }
            mfu_flist_metadata_apply(flist, create_opts);
        }
    } else {
        /* If we don't have offsets, have each process read the archive from the start.
         * We use libarchive to read/write entries, which allows us to deal with compressed
         * archives and those with things like global headers. */ 
        ret = extract_files(filename, flags, entries, entry_start, entry_count, flist, opts);
    }
    if (ret != MFU_SUCCESS) {
        /* set return code if we failed to extract items */
        if (mfu_rank == 0) {
            MFU_LOG(MFU_LOG_ERR, "Failed to extract all items");
        }
        rc = MFU_FAILURE;
    }

    /* If we extracted items with libarchive, we need to update timestamps on
     * any directories.  This is because we created all directories in advance
     * and libarchive does not set timestamps on directories if they already exist. */
    int extracted_with_libarchive = (!have_offsets || opts->extract_libarchive);
    if (extracted_with_libarchive) {
        /* first ensure all procs are done writing their items */
        MPI_Barrier(MPI_COMM_WORLD);
        if (mfu_rank == 0) {
            MFU_LOG(MFU_LOG_INFO, "Updating timestamps and permissions");
        }

        /* create a file list of just the directories */
        mfu_flist flist_dirs = mfu_flist_subset(flist);
        uint64_t idx;
        uint64_t size = mfu_flist_size(flist);
        for (idx = 0; idx < size; idx++) {
            /* if item is a directory, copy it to the directory list */
            mfu_filetype type = mfu_flist_file_get_type(flist, idx);
            if (type == MFU_TYPE_DIR) {
                mfu_flist_file_copy(flist, idx, flist_dirs);
            }
        }
        mfu_flist_summarize(flist_dirs);

        /* set timestamps on the directories, do this after writing all items
         * since creating items in a directory will have changed its timestamp */
        mfu_flist_metadata_apply(flist_dirs, create_opts);

        /* free the list of directories */
        mfu_flist_free(&flist_dirs);
    }

    /* if we constructed an offset list while unpacking the archive,
     * save it to an index file in case we need to unpack again */
    if (have_offsets && !have_index) {
        write_entry_index(filename, entry_count, &offsets[entry_start]);
    }

    /* free options structure needed for create calls */
    mfu_create_opts_delete(&create_opts);

    /* we can now free our file list */
    mfu_flist_free(&flist);

    /* free offset arrays */
    mfu_free(&data_offsets);
    mfu_free(&offsets);

    /* wait for all to finish */
    MPI_Barrier(MPI_COMM_WORLD);

    /* stop overall timer */
    time_t time_ended;
    time(&time_ended);
    double wtime_ended = MPI_Wtime();

    /* prep our values into buffer */
    int64_t values[2];
    values[0] = reduce_buf[REDUCE_ITEMS];
    values[1] = reduce_buf[REDUCE_BYTES];

    /* sum values across processes */
    int64_t sums[2];
    MPI_Allreduce(values, sums, 2, MPI_INT64_T, MPI_SUM, MPI_COMM_WORLD);

    /* extract results from allreduce */
    int64_t agg_items = sums[0];
    int64_t agg_bytes = sums[1];

    /* compute number of seconds */
    double secs = wtime_ended - wtime_started;

    /* compute rate of copy */
    double agg_bw = (double)agg_bytes / secs;
    if (secs > 0.0) {
        agg_bw = (double)agg_bytes / secs;
    }

    if(mfu_rank == 0) {
        /* format start time */
        char starttime_str[256];
        struct tm* localstart = localtime(&time_started);
        strftime(starttime_str, 256, "%b-%d-%Y, %H:%M:%S", localstart);

        /* format end time */
        char endtime_str[256];
        struct tm* localend = localtime(&time_ended);
        strftime(endtime_str, 256, "%b-%d-%Y, %H:%M:%S", localend);

        /* convert size to units */
        double agg_bytes_val;
        const char* agg_bytes_units;
        mfu_format_bytes((uint64_t)agg_bytes, &agg_bytes_val, &agg_bytes_units);

        /* convert bandwidth to units */
        double agg_bw_val;
        const char* agg_bw_units;
        mfu_format_bw(agg_bw, &agg_bw_val, &agg_bw_units);

        MFU_LOG(MFU_LOG_INFO, "Started:   %s", starttime_str);
        MFU_LOG(MFU_LOG_INFO, "Completed: %s", endtime_str);
        MFU_LOG(MFU_LOG_INFO, "Seconds: %.3lf", secs);
        MFU_LOG(MFU_LOG_INFO, "Items: %" PRId64, agg_items);
        MFU_LOG(MFU_LOG_INFO,
            "Data: %.3lf %s (%" PRId64 " bytes)",
            agg_bytes_val, agg_bytes_units, agg_bytes
        );
        MFU_LOG(MFU_LOG_INFO,
            "Rate: %.3lf %s (%.3" PRId64 " bytes in %.3lf seconds)",
            agg_bw_val, agg_bw_units, agg_bytes, secs
        );
    }

    return rc;
}

/* return a newly allocated archive_opts structure, set default values on its fields */
mfu_archive_opts_t* mfu_archive_opts_new(void)
{
    mfu_archive_opts_t* opts = (mfu_archive_opts_t*) MFU_MALLOC(sizeof(mfu_archive_opts_t));

    /* to record destination path that we'll be copying to */
    opts->dest_path = NULL;

    /* By default, don't bother to preserve all attributes. */
    opts->preserve = false;

    /* flags for libarchive */
    opts->flags = 0;

    /* size at which to slice up a file into units of work */
    opts->chunk_size = MFU_CHUNK_SIZE;

    /* buffer size for individual read/write operations */
    opts->buf_size = MFU_BLOCK_SIZE;

    /* whether to use libcircle (1) vs a static chunk list (0) when creating an archive */
    opts->create_libcircle   = 0;

    /* whether to extract items with libarchive (1) or read data from archive directly (0) */
    opts->extract_libarchive = 0;

    return opts;
}

void mfu_archive_opts_delete(mfu_archive_opts_t** popts)
{
  if (popts != NULL) {
    mfu_archive_opts_t* opts = *popts;

    /* free fields allocated on opts */
    if (opts != NULL) {
      mfu_free(&opts->dest_path);
    }

    mfu_free(popts);
  }
}