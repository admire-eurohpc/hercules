set_source_files_properties( ../src/imss.c ../src/hercules.c PROPERTIES LANGUAGE CXX)

add_executable(imssfs
    map.cpp
    imssfs.c
    imss_fuse_api.c
    mapprefetch.cpp
	../src/hercules.c
	../src/imss.c
    ../src/map_ep.cpp
    )


target_compile_definitions(imssfs PRIVATE  ENABLED_FUSE)   
target_compile_features(imssfs PRIVATE cxx_std_17)

set(CMAKE_C_FLAGS " -fPIC -D_FILE_OFFSET_BITS=64 -lulockmgr -DFUSE")

target_include_directories(imssfs PRIVATE ${FUSE_INCLUDE_DIR})
target_link_libraries(imssfs  ${CMAKE_THREAD_LIBS_INIT} ${FUSE_LIBRARIES}  imss_posix_api imss_static ucx::ucp ucx::ucs )

