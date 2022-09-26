set_source_files_properties(../src/comms.c ../src/crc.c ../src/directory.c ../src/hercules.c ../src/imss.c PROPERTIES LANGUAGE CXX )
set_source_files_properties(../src/memalloc.c ../src/policies.c ../src/stat.c ../src/workers.c ../src/arg_parser.c PROPERTIES LANGUAGE CXX )
set_source_files_properties(./server.c ./release_imss.c PROPERTIES LANGUAGE CXX )

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
		../src/arg_parser.c
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

SET(IMSS_INC "-I${CMAKE_SOURCE_DIR}/include")
SET(IMSS_LIB "-L${CMAKE_SOURCE_DIR}/build/app")

target_link_directories(server PUBLIC ${UCX_LIBRARIES})

TARGET_LINK_LIBRARIES(imss_static ${CMAKE_THREAD_LIBS_INIT} ucx::ucp ucx::ucs ${MPI_LIBRARIES} ${GLIB_LIBRARIES}  ) #${JEMALLOC_LIBRARIES})
TARGET_LINK_LIBRARIES(imss_shared ${CMAKE_THREAD_LIBS_INIT} ucx::ucp ucx::ucs ${MPI_LIBRARIES} ${GLIB_LIBRARIES} dl ) # ${JEMALLOC_LIBRARIES} )
TARGET_LINK_LIBRARIES(server ${CMAKE_THREAD_LIBS_INIT} ucx::ucp ucx::ucs ${MPI_LIBRARIES} ${GLIB_LIBRARIES} ${PCRE_LIBRARY}  ) # ${JEMALLOC_LIBRARIES} )
