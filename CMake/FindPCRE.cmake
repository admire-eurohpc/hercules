#
#  Copyright (C) 2008-2014 National Institute For Space Research (INPE) - Brazil.
#
#  This file is part of the TerraLib - a Framework for building GIS enabled applications.
#
#  TerraLib is free software: you can redistribute it and/or modify
#  it under the terms of the GNU Lesser General Public License as published by
#  the Free Software Foundation, either version 3 of the License,
#  or (at your option) any later version.
#
#  TerraLib is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
#  GNU Lesser General Public License for more details.
#
#  You should have received a copy of the GNU Lesser General Public License
#  along with TerraLib. See COPYING. If not, write to
#  TerraLib Team at <terralib-team@terralib.org>.
#
#
#  Description: Find Log4cxx - find Log4cxx include directory and library.
#
#  PCRE_INCLUDE_DIR - where to find log4cxx/log4cxx.h.
#  PCRE_LIBRARY     - where to find the log4cxx library.
#  PCRE_FOUND       - True if Log4cxx is found.
#
#  Author: Gilberto Ribeiro de Queiroz <gribeiro@dpi.inpe.br>
#          Juan P. Garrido <juan@dpi.inpe.br>
#
if(UNIX)
  find_path(PCRE_INCLUDE_DIR
          NAMES pcre.h
          PATHS /usr
                /usr/local
          PATH_SUFFIXES include)

  find_library(PCRE_LIBRARY
               NAMES pcre
               PATHS /usr
                     /usr/lib
					 /usr/lib/x86_64-linux-gnu
               PATH_SUFFIXES lib)
elseif(WIN32)
  find_path(PCRE_INCLUDE_DIR pcre.h)

  find_library(PCRE_LIBRARY_RELEASE pcre)

  if(PCRE_LIBRARY_RELEASE AND PCRE_LIBRARY_DEBUG)
    set(PCRE_LIBRARY optimized ${PCRE_LIBRARY_RELEASE} debug ${PCRE_LIBRARY_DEBUG})
  elseif(PCRE_LIBRARY_RELEASE)
    set(PCRE_LIBRARY optimized ${PCRE_LIBRARY_RELEASE} debug ${PCRE_LIBRARY_RELEASE})
  elseif(PCRE_LIBRARY_DEBUG)
    set(PCRE_LIBRARY optimized ${PCRE_LIBRARY_DEBUG} debug ${PCRE_LIBRARY_DEBUG})
  endif()
endif()
include(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(PCRE DEFAULT_MSG PCRE_LIBRARY PCRE_INCLUDE_DIR)
mark_as_advanced(PCRE_INCLUDE_DIR PCRE_LIBRARY)
