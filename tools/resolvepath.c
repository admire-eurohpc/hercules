/*
 * Copyright (c) 2003 Constantin S. Svintsoff <kostik@iclub.nsu.ru>
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. The names of the authors may not be used to endorse or promote
 *    products derived from this software without specific prior written
 *    permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

/* Last Update: 15/Dec/2023 */

#include "resolvepath.h"

/*
 * int ResolvePath(const char *path_, char *resolved)
 *
 * Find the real name of path, by removing all ".", "..".
 * Returns the size of the resolved path.
 */
int ResolvePath(const char *path_, char *resolved)
{
	char path[PATH_MAX];
	strcpy(path, path_);
	char *p, *q, *s;
	size_t left_len, resolved_len;
	unsigned symlinks;
	int serrno, slen;
	char left[PATH_MAX], next_token[PATH_MAX], symlink[PATH_MAX];

	serrno = errno;
	symlinks = 0;
	if (path[0] == '/')
	{
		resolved[0] = '/';
		resolved[1] = '\0';
		if (path[1] == '\0')
			return 1;
		resolved_len = 1;
		strncpy(left, path + 1, sizeof(left));
		left_len = strlen(left);
	}
	else
	{
		if (getcwd(resolved, PATH_MAX) == NULL)
		{
			strncpy(resolved, ".", strlen("."));
			return -1;
		}
		resolved_len = strlen(resolved);
		strncpy(left, path, sizeof(left));
		left_len = strlen(left);
	}
	if (left_len >= sizeof(left) || resolved_len >= PATH_MAX)
	{
		errno = ENAMETOOLONG;
		return -1;
	}

	/*
	 * Iterate over path components in `left'.
	 */
	while (left_len != 0)
	{
		/*
		 * Extract the next path component and adjust `left'
		 * and its length.
		 */
		p = strchr(left, '/');
		s = p ? p : left + left_len;
		if (s - left >= sizeof(next_token))
		{
			errno = ENAMETOOLONG;
			// return (NULL);
			return -1;
		}
		memcpy(next_token, left, s - left);
		next_token[s - left] = '\0';
		left_len -= s - left;
		if (p != NULL)
			memmove(left, s + 1, left_len + 1);
		if (resolved[resolved_len - 1] != '/')
		{
			if (resolved_len + 1 >= PATH_MAX)
			{
				errno = ENAMETOOLONG;
				// return (NULL);
				return -1;
			}
			resolved[resolved_len++] = '/';
			resolved[resolved_len] = '\0';
		}
		if (next_token[0] == '\0')
			continue;
		else if (strcmp(next_token, ".") == 0)
			continue;
		else if (strcmp(next_token, "..") == 0)
		{
			/*
			 * Strip the last path component except when we have
			 * single "/"
			 */
			if (resolved_len > 1)
			{
				resolved[resolved_len - 1] = '\0';
				q = strrchr(resolved, '/') + 1;
				if (q != NULL)
				{
					//  remove the last directory on the path component because "..".
					*q = '\0';
					resolved_len = q - resolved;
				}
			}
			continue;
		}

		/*
		 * Append the next path component and lstat() it. If
		 * lstat() fails we still can return successfully if
		 * there are no more path components left.
		 */
		strncat(resolved, next_token, strlen(next_token));
		resolved_len = strlen(resolved);
		if (resolved_len >= PATH_MAX)
		{
			errno = ENAMETOOLONG;
			// return (NULL);
			return -1;
		}
	}
	/*
	 * Remove trailing slash except when the resolved pathname
	 * is a single "/".
	 */
	if (resolved_len > 1 && resolved[resolved_len - 1] == '/')
	{
		resolved[resolved_len - 1] = '\0';
	}
	return resolved_len;
}