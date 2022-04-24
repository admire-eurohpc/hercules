set_source_files_properties(../src/comms.c ../src/crc.c ../src/directory.c ../src/hercules.c ../src/imss.c PROPERTIES LANGUAGE CXX )
set_source_files_properties(../src/memalloc.c ../src/policies.c ../src/stat.c ../src/workers.c PROPERTIES LANGUAGE CXX )
set_source_files_properties(./server.c ./release_imss.c PROPERTIES LANGUAGE CXX )
#set_source_files_properties(./dirr.c PROPERTIES LANGUAGE CXX )
#set_source_files_properties(./dummy.c PROPERTIES LANGUAGE CXX )
set_source_files_properties(./original_overhead.c PROPERTIES LANGUAGE CXX )
#set_source_files_properties(./gtype_gdeployed.c PROPERTIES LANGUAGE CXX )


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
#ADD_EXECUTABLE( dummy
#				./dummy.c
#)

ADD_EXECUTABLE( original_overhead
				./original_overhead.c
)
#
#ADD_EXECUTABLE( gtype_gdeployed 
#				./gtype_gdeployed.c
#)
#
#ADD_EXECUTABLE (dirr
#				./dirr.c
#				../src/directory.c
#)

SET(IMSS_INC "-I${CMAKE_SOURCE_DIR}/inc")
SET(IMSS_LIB "-L${CMAKE_SOURCE_DIR}/build/app")


target_link_directories(imss_static PUBLIC /opt/anaconda3/lib)
target_link_directories(imss_shared PUBLIC /opt/anaconda3/lib)
target_link_directories(server PUBLIC /opt/anaconda3/lib)

set(CMAKE_C_LDFLAGS "-L/opt/anaconda3/lib")

target_include_directories(imss_static PUBLIC zmq ) #${ZeroMQ_INCLUDE_DIR})
target_include_directories(imss_shared PUBLIC zmq ) #${ZeroMQ_INCLUDE_DIR})
target_include_directories(server PUBLIC  zmq) #${ZeroMQ_INCLUDE_DIR})
target_include_directories(release_imss PUBLIC  zmq ) #${ZeroMQ_INCLUDE_DIR})

TARGET_LINK_LIBRARIES(imss_static ${CMAKE_THREAD_LIBS_INIT} zmq  ${MPI_LIBRARIES} ${GLIB_LIBRARIES} )
#TARGET_LINK_LIBRARIES(imss_static ${CMAKE_THREAD_LIBS_INIT} zmq  ${MPI_LIBRARIES} ${GLIB_LIBRARIES}  tcmalloc )
#TARGET_LINK_LIBRARIES(imss_shared ${CMAKE_THREAD_LIBS_INIT} zmq  ${MPI_LIBRARIES} ${GLIB_LIBRARIES} tcmalloc)
TARGET_LINK_LIBRARIES(server ${CMAKE_THREAD_LIBS_INIT} zmq ${MPI_LIBRARIES} ${GLIB_LIBRARIES} ${PCRE_LIBRARY} )
#TARGET_LINK_LIBRARIES(dirr ${GLIB_LIBRARIES})
TARGET_LINK_LIBRARIES(release_imss zmq ${GLIB_LIBRARIES})
#TARGET_LINK_LIBRARIES(dummy imss_static ${CMAKE_THREAD_LIBS_INIT} zmq ${MPI_LIBRARIES} ${GLIB_LIBRARIES})
#TARGET_LINK_LIBRARIES(gtype_gdeployed imss_static ${CMAKE_THREAD_LIBS_INIT} zmq ${MPI_LIBRARIES} ${GLIB_LIBRARIES})
TARGET_LINK_LIBRARIES(original_overhead imss_static ${CMAKE_THREAD_LIBS_INIT} zmq ${GLIB_LIBRARIES})
