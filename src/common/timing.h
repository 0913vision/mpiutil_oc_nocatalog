#ifndef TIMING_H
#define TIMING_H

typedef struct {
    double total_time;
    int count;
} timing_info_t;

extern timing_info_t timing_info;
extern timing_info_t pread_timing_info;
extern timing_info_t md_timing_info;

void record_timing(double start, double end);
void pread_record_timing(double start, double end);
void md_record_timing(double start, double end);

#endif // TIMING_H
