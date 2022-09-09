set_source_files_properties(../src/comms.c ../src/crc.c ../src/directory.c ../src/hercules.c ../src/imss.c PROPERTIES LANGUAGE CXX )
set_source_files_properties(../src/memalloc.c ../src/policies.c ../src/stat.c ../src/workers.c PROPERTIES LANGUAGE CXX )
set_source_files_properties(./server.c ./release_imss.c PROPERTIES LANGUAGE CXX )
#set_source_files_properties(./dirr.c PROPERTIES LANGUAGE CXX )
#set_source_files_properties(./dummy.c PROPERTIES LANGUAGE CXX )
set_source_files_properties(./original_overhead.c PROPERTIES LANGUAGE CXX )
#set_source_files_properties(./gtype_gdeployed.c PROPERTIES LANGUAGE CXX )


ADD_EXECUTABLE( server 
		./server.c
		../src/stat.cpp
		../src/comms.c
		../src/queue.c
		../src/workers.c
		../src/memalloc.c
		../src/memalloc.c
		../src/directory.c
		../src/records.cpp
)

#ADD_EXECUTABLE( release_imss 
#				./release_imss.c
#				../src/comms.c
#)

ADD_LIBRARY( imss_static STATIC
		../src/crc.c
		../src/stat.cpp
		../src/imss.c
		../src/comms.c
		../src/workers.c
		../src/queue.c
		../src/hercules.c
		../src/policies.c
		../src/directory.c
		../src/records.cpp
)

ADD_LIBRARY( imss_shared SHARED
                ../src/crc.c
                ../src/stat.cpp
                ../src/imss.c
                ../src/comms.c
		../src/queue.c
                ../src/workers.c
                ../src/hercules.c
                ../src/policies.c
                ../src/directory.c
		../src/records.cpp
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

SET(IMSS_INC "-I${CMAKE_SOURCE_DIR}/include")
SET(IMSS_LIB "-L${CMAKE_SOURCE_DIR}/build/app")

target_link_directories(server PUBLIC ${UCX_LIBRARIES})
target_link_directories(original_overhead PUBLIC ${UCX_LIBRARIES})

TARGET_LINK_LIBRARIES(imss_static ${CMAKE_THREAD_LIBS_INIT} ucp ucs  ${MPI_LIBRARIES} ${GLIB_LIBRARIES} ${JEMALLOC_LIBRARIES})
TARGET_LINK_LIBRARIES(imss_shared ${CMAKE_THREAD_LIBS_INIT} ucp ucs ${MPI_LIBRARIES} ${GLIB_LIBRARIES} ${JEMALLOC_LIBRARIES} dl)
TARGET_LINK_LIBRARIES(server ${CMAKE_THREAD_LIBS_INIT} ucx::ucp ucx::ucs ${MPI_LIBRARIES} ${GLIB_LIBRARIES} ${PCRE_LIBRARY} ${JEMALLOC_LIBRARIES} )
TARGET_LINK_LIBRARIES(original_overhead imss_static ${CMAKE_THREAD_LIBS_INIT}  ucs ucp ${GLIB_LIBRARIES})
