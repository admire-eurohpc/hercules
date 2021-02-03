set_source_files_properties(../src/comms.c ../src/crc.c ../src/directory.c ../src/hercules.c ../src/imss.c PROPERTIES LANGUAGE CXX )
set_source_files_properties(../src/memalloc.c ../src/policies.c ../src/stat.c ../src/workers.c PROPERTIES LANGUAGE CXX )
set_source_files_properties(./server.c ./release_imss.c PROPERTIES LANGUAGE CXX )

set_source_files_properties(collective_write.c PROPERTIES LANGUAGE CXX )
set_source_files_properties(individual_write.c PROPERTIES LANGUAGE CXX )

#set_source_files_properties(router_test.c PROPERTIES LANGUAGE CXX )

#find_package(PkgConfig)
#
#pkg_check_modules(PC_ZeroMQ QUIET zmq)
#
#find_path(ZeroMQ_INCLUDE_DIR
#        NAMES zmq.hpp
#        PATHS ${PC_ZeroMQ_INCLUDE_DIRS}
#        )
#
#find_library(ZeroMQ_LIBRARY
#        NAMES zmq
#        PATHS ${PC_ZeroMQ_LIBRARY_DIRS}
#        )

ADD_EXECUTABLE( server 
				./server.c
				../src/stat.c
				../src/comms.c
				../src/workers.c
				../src/memalloc.c
				../src/directory.c
)

ADD_EXECUTABLE( release_imss 
				./release_imss.c
)

#ADD_EXECUTABLE( router_test
#				./router_test.c
#)

ADD_LIBRARY( imss_static STATIC
				../src/crc.c
				../src/stat.c
				../src/imss.c
				../src/comms.c
				../src/workers.c
				../src/hercules.c
				../src/policies.c
				../src/directory.c
)

ADD_LIBRARY( imss_shared SHARED
                ../src/crc.c
                ../src/stat.c
                ../src/imss.c
                ../src/comms.c
                ../src/workers.c
                ../src/hercules.c
                ../src/policies.c
                ../src/directory.c
)

#ADD_EXECUTABLE( collective_write 
#				./collective_write.c
#)
#ADD_EXECUTABLE( individual_write 
#				./individual_write.c
#)

SET(IMSS_INC "-I${CMAKE_SOURCE_DIR}/inc")
SET(IMSS_LIB "-L${CMAKE_SOURCE_DIR}/build/app")

target_include_directories(imss_static PUBLIC zmq ) #${ZeroMQ_INCLUDE_DIR})
target_include_directories(server PUBLIC  zmq ) #${ZeroMQ_INCLUDE_DIR})
target_include_directories(release_imss PUBLIC  zmq ) #${ZeroMQ_INCLUDE_DIR})
#target_include_directories(router_test PUBLIC  zmq ) #${ZeroMQ_INCLUDE_DIR})

TARGET_LINK_LIBRARIES(server ${CMAKE_THREAD_LIBS_INIT} zmq ${MPI_LIBRARIES} ${GLIB_LIBRARIES})
TARGET_LINK_LIBRARIES(release_imss zmq)
TARGET_LINK_LIBRARIES(imss zmq)
#TARGET_LINK_LIBRARIES(collective_write imss ${CMAKE_THREAD_LIBS_INIT} zmq ${MPI_LIBRARIES} ${GLIB_LIBRARIES} )
#TARGET_LINK_LIBRARIES(individual_write imss ${CMAKE_THREAD_LIBS_INIT} zmq ${MPI_LIBRARIES} ${GLIB_LIBRARIES} )
#TARGET_LINK_LIBRARIES(router_test zmq ${MPI_LIBRARIES} )
