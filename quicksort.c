/*
 * quicksort.c
 *
 * CSCI 243 -- Homework 8: Threaded and Non-threaded Quicksort
 *
 * Author: Munkh-Orgil Jargalsaikhan
 * Date:   December 5, 2025
 *
 * This program implements Quicksort in two versions:
 *   1. Classic recursive (single-threaded)
 *   2. Multi-threaded using POSIX threads
 *
 * Both versions allocate and return a new sorted array.
 * The program reads integers from a file and reports CPU time
 * and number of threads spawned in the threaded version.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <time.h>

//#define _GNU_SOURCE

/// Structure passed to each thread in the threaded quicksort
typedef struct {
    size_t   size;     ///< number of elements to sort
    const int *data;   ///< pointer to original unsorted data
    int      *result;  ///< where to store sorted result (output)
    int       threads_spawned; ///< total threads created in subtree
} thread_args_t;

/// Global counter for total threads created (updated atomically)
static volatile int total_threads = 0;

/// Increments the global thread counter in a thread-safe way
static void increment_thread_counter( void )
{
    __sync_fetch_and_add( &total_threads, 1 );
}

/*
 * Helper functions (all static)
 */

/// Partitions array around pivot into less, same, and more lists
static void partition_array( int pivot,
                             size_t size,
                             const int *data,
                             size_t *less_cnt,
                             int **less,
                             size_t *same_cnt,
                             int **same,
                             size_t *more_cnt,
                             int **more )
{
    *less_cnt = *same_cnt = *more_cnt = 0;

    /* First pass: count how many in each category */
    for ( size_t i = 0; i < size; i++ ) {
        if ( data[i] < pivot )      ( *less_cnt )++;
        else if ( data[i] == pivot ) ( *same_cnt )++;
        else                         ( *more_cnt )++;
    }

    *less = malloc( *less_cnt * sizeof( int ) );
    *same = malloc( *same_cnt * sizeof( int ) );
    *more = malloc( *more_cnt * sizeof( int ) );

    if ( ( *less_cnt > 0 && *less == NULL ) ||
         ( *same_cnt > 0 && *same == NULL ) ||
         ( *more_cnt > 0 && *more == NULL ) ) {
        perror( "malloc failed in partition_array" );
        exit( EXIT_FAILURE );
    }

    size_t l_idx = 0, s_idx = 0, m_idx = 0;
    for ( size_t i = 0; i < size; i++ ) {
        if ( data[i] < pivot )
            ( *less )[ l_idx++ ] = data[i];
        else if ( data[i] == pivot )
            ( *same )[ s_idx++ ] = data[i];
        else
            ( *more )[ m_idx++ ] = data[i];
    }
}

/// Merges three sorted partitions into one newly allocated array
static int *merge_partitions( size_t less_cnt, const int *less,
                              size_t same_cnt, const int *same,
                              size_t more_cnt, const int *more )
{
    size_t total = less_cnt + same_cnt + more_cnt;
    int *result = malloc( total * sizeof( int ) );

    if ( total > 0 && result == NULL ) {
        perror( "malloc failed in merge_partitions" );
        exit( EXIT_FAILURE );
    }

    memcpy( result,              less, less_cnt * sizeof( int ) );
    memcpy( result + less_cnt,   same, same_cnt * sizeof( int ) );
    memcpy( result + less_cnt + same_cnt, more, more_cnt * sizeof( int ) );

    return result;
}

/// Prints array as comma-separated values (no trailing comma or newline)
static void print_array( const int *arr, size_t size )
{
    for ( size_t i = 0; i < size; i++ ) {
        if ( i > 0 ) printf( ", " );
        printf( "%d", arr[i] );
    }
}

/*
 * Non-threaded recursive quicksort
 */

static int *recursive_quicksort( size_t size, const int *data );

/// Public entry point for non-threaded quicksort (resets thread counter)
/// @param size number of elements
/// @param data original array
/// @return newly allocated sorted array (caller must free)
int *quicksort( size_t size, const int *data )
{
    total_threads = 0;                     // not used here, but reset anyway
    return recursive_quicksort( size, data );
}

static int *recursive_quicksort( size_t size, const int *data )
{
    if ( size <= 1 ) {
        int *res = malloc( size * sizeof( int ) );
        if ( size == 1 ) res[0] = data[0];
        return res;
    }

    int pivot = data[0];
    size_t less_cnt, same_cnt, more_cnt;
    int *less = NULL, *same = NULL, *more = NULL;

    partition_array( pivot, size, data,
                     &less_cnt, &less, &same_cnt, &same, &more_cnt, &more );

    int *sorted_less = recursive_quicksort( less_cnt, less );
    int *sorted_more = recursive_quicksort( more_cnt, more );

    int *result = merge_partitions( less_cnt, sorted_less,
                                    same_cnt, same,
                                    more_cnt, sorted_more );

    free( less );  free( same );  free( more );
    free( sorted_less );
    free( sorted_more );

    return result;
}

/*
 * Threaded quicksort
 */

/// Thread start routine – performs quicksort on its partition
/// @param arg pointer to thread_args_t
/// @return pointer to sorted array (cast to void*)
static void *threaded_quicksort( void *arg )
{
    thread_args_t *args = (thread_args_t *) arg;
    size_t size = args->size;
    const int *data = args->data;

    if ( size <= 1 ) {
        int *res = malloc( size * sizeof( int ) );
        if ( size == 1 ) res[0] = data[0];
        args->result = res;
        return (void *) res;
    }

    int pivot = data[0];
    size_t less_cnt, same_cnt, more_cnt;
    int *less = NULL, *same = NULL, *more = NULL;

    partition_array( pivot, size, data,
                     &less_cnt, &less, &same_cnt, &same, &more_cnt, &more );

    int *sorted_less = NULL;
    int *sorted_more = NULL;
    pthread_t thread_less, thread_more;
    int spawned_less = 0, spawned_more = 0;

    thread_args_t child_less = { less_cnt, less, NULL, 0 };
    thread_args_t child_more = { more_cnt, more, NULL, 0 };

    if ( less_cnt > 0 ) {
        if ( pthread_create( &thread_less, NULL, threaded_quicksort,
                             &child_less ) == 0 ) {
            increment_thread_counter();
            spawned_less = 1;
        } else {
            sorted_less = recursive_quicksort( less_cnt, less );
        }
    } else {
        sorted_less = NULL;
    }

    if ( more_cnt > 0 ) {
        if ( pthread_create( &thread_more, NULL, threaded_quicksort,
                             &child_more ) == 0 ) {
            increment_thread_counter();
            spawned_more = 1;
        } else {
            sorted_more = recursive_quicksort( more_cnt, more );
        }
    } else {
        sorted_more = NULL;
    }

    if ( spawned_less ) {
        void *ret;
        pthread_join( thread_less, &ret );
        sorted_less = (int *) ret;
    }
    if ( spawned_more ) {
        void *ret;
        pthread_join( thread_more, &ret );
        sorted_more = (int *) ret;
    }

    int *result = merge_partitions( less_cnt, sorted_less,
                                    same_cnt, same,
                                    more_cnt, sorted_more );

    free( less );  free( same );  free( more );
    if ( sorted_less && less_cnt > 0 ) free( sorted_less );
    if ( sorted_more && more_cnt > 0 ) free( sorted_more );

    args->result = result;
    args->threads_spawned = spawned_less + spawned_more;

    return (void *) result;
}

/*
 * File I/O
 */

/// Reads all integers from file, one per line
/// @param filename name of input file
/// @param out_size filled with number of integers read
/// @return dynamically allocated array (caller must free)
static int *read_integers_from_file( const char *filename, size_t *out_size )
{
    FILE *fp = fopen( filename, "r" );
    if ( fp == NULL ) {
        perror( "fopen" );
        exit( EXIT_FAILURE );
    }

    size_t capacity = 256;
    int *data = malloc( capacity * sizeof( int ) );
    if ( data == NULL ) {
        perror( "malloc" );
        fclose( fp );
        exit( EXIT_FAILURE );
    }

    size_t count = 0;
    int value;

    while ( fscanf( fp, "%d", &value ) == 1 ) {
        if ( count >= capacity ) {
            capacity *= 2;
            data = realloc( data, capacity * sizeof( int ) );
            if ( data == NULL ) {
                perror( "realloc" );
                fclose( fp );
                exit( EXIT_FAILURE );
            }
        }
        data[count++] = value;

        /* consume rest of line */
        int ch;
        while ( ( ch = fgetc( fp ) ) != '\n' && ch != EOF )
            ;
    }

    fclose( fp );
    *out_size = count;
    return data;
}

/*
 * main
 */

/// Program entry point
/// @param argc argument count
/// @param argv arguments: [-p] filename
/// @return EXIT_SUCCESS or EXIT_FAILURE
int main( int argc, char *argv[] )
{
    int print_lists = 0;

    int opt;
    while ( ( opt = getopt( argc, argv, "p" ) ) != -1 ) {
        switch ( opt ) {
            case 'p':
                print_lists = 1;
                break;
            default:
                fprintf( stderr, "Usage: %s [-p] file_of_integers\n",
                         argv[0] );
                return EXIT_FAILURE;
        }
    }

    if ( optind >= argc ) {
        fprintf( stderr, "usage: %s [-p] file_of_ints\n", argv[0] );        return EXIT_FAILURE;
    }
    if ( optind < argc - 1 ) {
        fprintf( stderr, "Error: extra arguments after filename\n" );
        return EXIT_FAILURE;
    }

    char *filename = argv[optind];
    size_t num_elements;
    int *original_data = read_integers_from_file( filename, &num_elements );

    if ( num_elements == 0 ) {
        fprintf( stderr, "Error: no integers found in file\n" );
        free( original_data );
        return EXIT_FAILURE;
    }

    clock_t start, end;
    double cpu_time;

    /* Non-threaded version */
    if ( print_lists ) {
        printf( "Unsorted list before non-threaded quicksort:  " );
        print_array( original_data, num_elements );
        printf( "\n" );
    }

    start = clock();
    int *sorted1 = quicksort( num_elements, original_data );
    end = clock();
    cpu_time = (double)( end - start ) / CLOCKS_PER_SEC;

    printf( "Non-threaded time:  %f\n", cpu_time );

    if ( print_lists ) {
        printf( "Resulting list:  " );
        print_array( sorted1, num_elements );
        printf( "\n" );
    }
    free( sorted1 );

    /* Threaded version */
    if ( print_lists ) {
        printf( "Unsorted list before threaded quicksort:  " );
        print_array( original_data, num_elements );
        printf( "\n" );
    }

    total_threads = 0;

    thread_args_t top_args = { num_elements, original_data, NULL, 0 };
    pthread_t top_thread;

    start = clock();
    if ( pthread_create( &top_thread, NULL, threaded_quicksort,
                         &top_args ) != 0 ) {
        perror( "pthread_create top level" );
        free( original_data );
        return EXIT_FAILURE;
    }
    increment_thread_counter();   // count the top-level thread

    void *thread_ret;
    pthread_join( top_thread, &thread_ret );
    end = clock();

    cpu_time = (double)( end - start ) / CLOCKS_PER_SEC;

    printf( "Threaded time:      %f\n", cpu_time );
    printf( "Threads spawned:    %d\n", total_threads );

    if ( print_lists ) {
        printf( "Resulting list:  " );
        print_array( top_args.result, num_elements );
        printf( "\n" );
    }

    free( top_args.result );
    free( original_data );

    return EXIT_SUCCESS;
}