
add_executable(mount.imss
    mount.c
    )

add_executable(umount.imss
    umount.c
    )


add_library (imss_posix SHARED imss_posix.c mapfd.cpp ../fuse/map.cpp) 

#set(CMAKE_C_FLAGS "-fPIC -D_FILE_OFFSET_BITS=64 -lulockmgr -DFUSE")
set(CMAKE_C_FLAGS "-fPIC -lulockmgr -DFUSE")


target_include_directories(imss_posix PRIVATE ../fuse  .)

target_link_libraries(mount.imss  ${CMAKE_THREAD_LIBS_INIT} zmq ${GLIB_LIBRARIES} ${FUSE_LIBRARIES} imss_static)
target_link_libraries(umount.imss ${CMAKE_THREAD_LIBS_INIT} zmq ${GLIB_LIBRARIES} ${FUSE_LIBRARIES} imss_static)
target_link_libraries(imss_posix  ${CMAKE_THREAD_LIBS_INIT} zmq ${GLIB_LIBRARIES} ${FUSE_LIBRARIES} imss_posix_api imss_static dl)
