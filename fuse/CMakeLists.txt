find_package(FUSE REQUIRED)

add_executable(imssfs
    map.cpp
    imssfs.c
    )

set(CMAKE_C_FLAGS "-D_FILE_OFFSET_BITS=64 -lulockmgr -DFUSE")
set(CMAKE_C_LDFLAGS "-L/opt/anaconda3/lib")



target_include_directories(imssfs PRIVATE ${FUSE_INCLUDE_DIR})
target_link_libraries(imssfs  ${CMAKE_THREAD_LIBS_INIT} zmq ${GLIB_LIBRARIES} ${FUSE_LIBRARIES} imss_static tcmalloc)
