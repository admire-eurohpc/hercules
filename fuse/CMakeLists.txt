find_package(FUSE REQUIRED)

add_executable(imssfs
    imss.c
    )

set(CMAKE_C_FLAGS "-D_FILE_OFFSET_BITS=64 -lulockmgr -DFUSE")

target_include_directories(imssfs PRIVATE ${FUSE_INCLUDE_DIR})
target_link_libraries(imssfs  ${CMAKE_THREAD_LIBS_INIT} zmq ${GLIB_LIBRARIES} ${FUSE_LIBRARIES} imss_static)
