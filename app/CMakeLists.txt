set_source_files_properties(buffer.c stat_server.c ../src/stat.c PROPERTIES LANGUAGE CXX )
set_source_files_properties(../src/buff_workers.c ../src/memalloc.c PROPERTIES LANGUAGE CXX )
set_source_files_properties(../src/stat_workers.c PROPERTIES LANGUAGE CXX )

ADD_EXECUTABLE( buffer
				buffer.c	
				../src/buff_workers.c
				../src/memalloc.c
)

ADD_EXECUTABLE( stat_srv 
				stat_server.c
				../src/stat.c
				../src/stat_workers.c
				../src/memalloc.c
)

ADD_LIBRARY( imss SHARED
				../src/imss.c
				../src/policies.c
				../src/crc.c
)

SET(IMSS_INC "-I${CMAKE_SOURCE_DIR}/inc")
SET(IMSS_LIB "-L${CMAKE_SOURCE_DIR}/build/app")

ADD_EXECUTABLE( client
				../app/client.c
)

TARGET_LINK_LIBRARIES(buffer ${CMAKE_THREAD_LIBS_INIT} zmq ${MPI_LIBRARIES})
TARGET_LINK_LIBRARIES(stat_srv  ${CMAKE_THREAD_LIBS_INIT} zmq ${MPI_LIBRARIES})
TARGET_LINK_LIBRARIES(imss zmq ${MPI_LIBRARIES} )
TARGET_LINK_LIBRARIES(client imss ${MPI_LIBRARIES} )

