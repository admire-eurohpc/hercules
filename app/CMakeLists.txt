set_source_files_properties(../src/comms.c ../src/crc.c ../src/directory.c ../src/hercules.c ../src/imss.c PROPERTIES LANGUAGE CXX )
set_source_files_properties(../src/memalloc.c ../src/policies.c ../src/stat.c ../src/workers.c PROPERTIES LANGUAGE CXX )
set_source_files_properties(./server.c ./release_imss.c PROPERTIES LANGUAGE CXX )
set_source_files_properties(./dirr.c PROPERTIES LANGUAGE CXX )


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

ADD_LIBRARY( imss SHARED
				../src/crc.c
				../src/stat.c
				../src/imss.c
				../src/comms.c
				../src/workers.c
				../src/hercules.c
				../src/policies.c
				../src/directory.c
)

ADD_EXECUTABLE (dirr
				./dirr.c
				../src/directory.c
)

SET(IMSS_INC "-I${CMAKE_SOURCE_DIR}/inc")
SET(IMSS_LIB "-L${CMAKE_SOURCE_DIR}/build/app")

target_include_directories(imss PUBLIC zmq ) #${ZeroMQ_INCLUDE_DIR})
target_include_directories(server PUBLIC  zmq ) #${ZeroMQ_INCLUDE_DIR})
target_include_directories(release_imss PUBLIC  zmq ) #${ZeroMQ_INCLUDE_DIR})

TARGET_LINK_LIBRARIES(imss ${CMAKE_THREAD_LIBS_INIT} zmq ${MPI_LIBRARIES} ${GLIB_LIBRARIES})
TARGET_LINK_LIBRARIES(server ${CMAKE_THREAD_LIBS_INIT} zmq ${MPI_LIBRARIES} ${GLIB_LIBRARIES})
TARGET_LINK_LIBRARIES(dirr ${GLIB_LIBRARIES})
TARGET_LINK_LIBRARIES(release_imss zmq)
