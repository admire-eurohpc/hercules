/* Copyright (C) 1991-2022 Free Software Foundation, Inc.
   This file is part of the GNU C Library.
   The GNU C Library is free software; you can redistribute it and/or
   modify it under the terms of the GNU Lesser General Public
   License as published by the Free Software Foundation; either
   version 2.1 of the License, or (at your option) any later version.
   The GNU C Library is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
   Lesser General Public License for more details.
   You should have received a copy of the GNU Lesser General Public
   License along with the GNU C Library; if not, see
   <https://www.gnu.org/licenses/>.  */

/* Last Update: 26/Sep/2023 */

#include <stdint.h>
#include <string.h>
#include <stddef.h>
#include <stdbool.h>
#include <errno.h>
#include <stdalign.h>
#include <sys/random.h>

#define struct_stat64 struct stat
#define __lstat64_time64(file, buf) lstat(file, buf)
#define __getrandom getrandom

/* Use getrandom if it works, falling back on a 64-bit linear
   congruential generator that starts with Var's value
   mixed in with a clock's low-order bits if available.  */
typedef uint_fast64_t random_value;

#define RANDOM_VALUE_MAX UINT_FAST64_MAX
#define BASE_62_DIGITS 10 /* 62**10 < UINT_FAST64_MAX */
#define BASE_62_POWER (62LL * 62 * 62 * 62 * 62 * 62 * 62 * 62 * 62 * 62)

static random_value
random_bits(random_value var, bool use_getrandom)
{
    random_value r;
    /* Without GRND_NONBLOCK it can be blocked for minutes on some systems.  */
    if (use_getrandom && __getrandom(&r, sizeof r, GRND_NONBLOCK) == sizeof r)
        return r;
#if _LIBC || (defined CLOCK_MONOTONIC && HAVE_CLOCK_GETTIME)
    /* Add entropy if getrandom did not work.  */
    struct __timespec64 tv;
    __clock_gettime64(CLOCK_MONOTONIC, &tv);
    var ^= tv.tv_nsec;
#endif
    return 2862933555777941757 * var + 3037000493;
}

static int try_nocreate(char *tmpl, void *flags)
{
    struct_stat64 st;
    if (__lstat64_time64(tmpl, &st) == 0 || errno == EOVERFLOW)
        errno = EEXIST;
    return errno == ENOENT ? 0 : -1;
}

/* These are the characters used in temporary file names.  */
static const char letters[] =
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

int try_tempname_len(char *tmpl)
{

    size_t len;
    char *XXXXXX;
    unsigned int count;
    int fd = -1;

    // The following variables are passed as args. For testing I will define them here.
    int suffixlen = 0;
    int (*tryfunc)(char *, void *);
    size_t x_suffix_len = 6;

    /* A lower bound on the number of temporary files to attempt to
       generate.  The maximum total number of temporary file names that
       can exist for a given template is 62**6.  It should never be
       necessary to try all of these combinations.  Instead if a reasonable
       number of names is tried (we define reasonable as 62**3) fail to
       give the system administrator the chance to remove the problems.
       This value requires that X_SUFFIX_LEN be at least 3.  */
#define ATTEMPTS_MIN (62 * 62 * 62)
    unsigned int attempts = ATTEMPTS_MIN;

    /* A random variable.  The initial value is used only the for fallback path
       on 'random_bits' on 'getrandom' failure.  Its initial value tries to use
       some entropy from the ASLR and ignore possible bits from the stack
       alignment.  */
    random_value v = ((uintptr_t)&v) / alignof(max_align_t);
    /* How many random base-62 digits can currently be extracted from V.  */
    int vdigits = 0;
    /* Whether to consume entropy when acquiring random bits.  On the
       first try it's worth the entropy cost with __GT_NOCREATE, which
       is inherently insecure and can use the entropy to make it a bit
       less secure.  On the (rare) second and later attempts it might
       help against DoS attacks.  */
    bool use_getrandom = tryfunc == try_nocreate;
    /* Least unfair value for V.  If V is less than this, V can generate
       BASE_62_DIGITS digits fairly.  Otherwise it might be biased.  */
    random_value const unfair_min = RANDOM_VALUE_MAX - RANDOM_VALUE_MAX % BASE_62_POWER;
    len = strlen(tmpl);
    if (len < x_suffix_len + suffixlen || strspn(&tmpl[len - x_suffix_len - suffixlen], "X") < x_suffix_len)
    {
        errno = EINVAL;
        return -1;
    }
    /* This is where the Xs start.  */
    XXXXXX = &tmpl[len - x_suffix_len - suffixlen];
    for (count = 0; count < attempts; ++count)
    {
        for (size_t i = 0; i < x_suffix_len; i++)
        {
            if (vdigits == 0)
            {
                do
                {
                    v = random_bits(
                        v, use_getrandom);
                    use_getrandom = true;
                } while (unfair_min <= v);
                vdigits = BASE_62_DIGITS;
            }
            XXXXXX[i] = letters[v % 62];
            v /= 62;
            vdigits--;
        }
        // fd = tryfunc(tmpl, args);
        // if (fd >= 0)
        // {
        //     errno =save_errno;
        //     return fd;
        // }
        // else if (errno != EEXIST)
        //     return -1;
    }
}
