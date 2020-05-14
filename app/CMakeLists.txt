#set_source_files_properties(buffer.c stat_server.c PROPERTIES LANGUAGE CXX )
set_source_files_properties(../src/comms.c ../src/crc.c ../src/imss.c ../src/memalloc.c PROPERTIES LANGUAGE CXX )
set_source_files_properties(../src/policies.c ../src/server.c ../src/stat.c ../src/workers.c PROPERTIES LANGUAGE CXX )


ADD_EXECUTABLE( server 
				../src/server.c
				../src/workers.c
				../src/stat.c
				../src/comms.c
				../src/memalloc.c
)

ADD_LIBRARY( imss SHARED
				../src/imss.c
				../src/policies.c
				../src/crc.c
				../src/comms.c
)

SET(IMSS_INC "-I${CMAKE_SOURCE_DIR}/inc")
SET(IMSS_LIB "-L${CMAKE_SOURCE_DIR}/build/app")

#ADD_EXECUTABLE( client
#				../app/client.c
#)

TARGET_LINK_LIBRARIES(server ${CMAKE_THREAD_LIBS_INIT} zmq ${MPI_LIBRARIES} ${GLIB_LIBRARIES})
#TARGET_LINK_LIBRARIES(imss zmq ${MPI_LIBRARIES} )
#TARGET_LINK_LIBRARIES(client imss ${MPI_LIBRARIES} )

