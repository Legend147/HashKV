/**
 * minunit.h
 * Written by Michael Barker and released to the public domain,
 * as explained at http://creativecommons.org/publicdomain/zero/1.0/
 */

#ifndef MINUNIT_H
#define MINUNIT_H

#include <math.h>
#include <inttypes.h>

struct mu_result
{
    char* test;
    char* message;
};

#define mu_assert(message, test) \
    do {                         \
        if (!(test))             \
            return message;      \
    } while (0)

#define mu_run_test(name)        \
    do {                         \
        char *message = name();  \
        tests_run++;             \
        if (message) {           \
            struct mu_result r;  \
            r.test = #name;      \
            r.message = message; \
            return r;            \
        }                        \
    } while (0)

#define mu_ok               \
    do {                    \
        struct mu_result r; \
        r.test = 0;         \
        r.message = 0;      \
        return r;           \
    } while (0)

extern int tests_run;

static bool compare_double(double a, double b, double delta)
{
    if (fabs(a - b) < delta)
    {
        return true;
    }

    printf("[compare_double] fabs(%f, %f) < %f == false\n", a, b, delta);
    return false;
}

static bool compare_int64(int64_t a, int64_t b)
{
    if (a == b)
    {
        return true;
    }

    printf("[compare_int64] %" PRIu64 " == %" PRIu64 " == false\n", a, b);
    return false;
}

#endif
