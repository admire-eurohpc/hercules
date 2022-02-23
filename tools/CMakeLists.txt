
add_executable(mount.imss
    mount.c
    )

add_executable(umount.imss
    umount.c
    )

add_executable(kk
    kk.c
    )

add_library (imss_posix SHARED imss_posix.c) 

set(CMAKE_C_FLAGS "-D_FILE_OFFSET_BITS=64 -lulockmgr -DFUSE")

target_link_libraries(mount.imss  ${CMAKE_THREAD_LIBS_INIT} zmq ${GLIB_LIBRARIES} ${FUSE_LIBRARIES} imss_static)
target_link_libraries(kk  ${CMAKE_THREAD_LIBS_INIT} zmq ${GLIB_LIBRARIES} ${FUSE_LIBRARIES} imss_static)
target_link_libraries(umount.imss  ${CMAKE_THREAD_LIBS_INIT} zmq ${GLIB_LIBRARIES} ${FUSE_LIBRARIES} imss_static)
target_link_libraries(imss_posix  ${CMAKE_THREAD_LIBS_INIT} zmq ${GLIB_LIBRARIES} ${FUSE_LIBRARIES} imss_static)
