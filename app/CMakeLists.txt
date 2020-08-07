#set_source_files_properties(buffer.c stat_server.c PROPERTIES LANGUAGE CXX )
set_source_files_properties(../src/comms.c ../src/crc.c ../src/imss.c ../src/memalloc.c PROPERTIES LANGUAGE CXX )
set_source_files_properties(../src/policies.c ../src/server.c ../src/stat.c ../src/workers.c PROPERTIES LANGUAGE CXX )
set_source_files_properties(collective_write.c PROPERTIES LANGUAGE CXX )
set_source_files_properties(getdatalocation.c PROPERTIES LANGUAGE CXX )

pkg_check_modules(PC_ZeroMQ QUIET zmq)

find_path(ZeroMQ_INCLUDE_DIR
        NAMES zmq.hpp
        PATHS ${PC_ZeroMQ_INCLUDE_DIRS}
        )

find_library(ZeroMQ_LIBRARY
        NAMES zmq
        PATHS ${PC_ZeroMQ_LIBRARY_DIRS}
        )



ADD_EXECUTABLE( server 
				../src/server.c
				../src/workers.c
				../src/stat.c
				../src/comms.c
				../src/memalloc.c
)

ADD_EXECUTABLE( release_imss 
				../src/release_imss.c
)

ADD_LIBRARY( imss SHARED
				../src/imss.c
				../src/policies.c
				../src/crc.c
				../src/comms.c
)

ADD_EXECUTABLE( collective_write 
				./collective_write.c
)

ADD_EXECUTABLE( getdatalocation 
				./getdatalocation.c
)


SET(IMSS_INC "-I${CMAKE_SOURCE_DIR}/inc")
SET(IMSS_LIB "-L${CMAKE_SOURCE_DIR}/build/app")

#ADD_EXECUTABLE( client
#				../app/client.c
#)
target_include_directories(imss PUBLIC ${ZeroMQ_INCLUDE_DIR})
target_include_directories(server PUBLIC ${ZeroMQ_INCLUDE_DIR})


TARGET_LINK_LIBRARIES(server ${CMAKE_THREAD_LIBS_INIT} zmq ${MPI_LIBRARIES} ${GLIB_LIBRARIES})
TARGET_LINK_LIBRARIES(release_imss ${ZeroMQ_LIBRARY} ${MPI_LIBRARIES})
TARGET_LINK_LIBRARIES(collective_write imss ${CMAKE_THREAD_LIBS_INIT} ${ZeroMQ_LIBRARY} ${MPI_LIBRARIES} ${GLIB_LIBRARIES} )
TARGET_LINK_LIBRARIES(getdatalocation imss ${ZeroMQ_LIBRARY} ${GLIB_LIBRARIES} )
#TARGET_LINK_LIBRARIES(imss zmq ${MPI_LIBRARIES} )
#TARGET_LINK_LIBRARIES(client imss ${MPI_LIBRARIES} )

