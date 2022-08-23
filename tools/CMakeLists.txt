add_executable(mount.imss
    mount.c
    )

add_executable(umount.imss
    umount.c
    )

set(CMAKE_C_FLAGS "-fPIC -lulockmgr -DFUSE")
set(CMAKE_CXX_FLAGS "-fPIC -lulockmgr -DFUSE")

add_library (imss_posix SHARED imss_posix.c mapfd.cpp ../fuse/map.cpp) 

target_include_directories(imss_posix PRIVATE ../fuse  .)

target_link_directories(imss_posix PUBLIC /opt/anaconda3/lib ${UCX_LIBRARIES})
target_link_directories(mount.imss PUBLIC /opt/anaconda3/lib ${UCX_LIBRARIES})
target_link_directories(umount.imss PUBLIC /opt/anaconda3/lib ${UCX_LIBRARIES})

target_link_libraries(mount.imss  ${CMAKE_THREAD_LIBS_INIT} ${GLIB_LIBRARIES} ${FUSE_LIBRARIES} imss_static)
target_link_libraries(umount.imss ${CMAKE_THREAD_LIBS_INIT} ${GLIB_LIBRARIES} ${FUSE_LIBRARIES} imss_static)
target_link_libraries(imss_posix  ${CMAKE_THREAD_LIBS_INIT} ${GLIB_LIBRARIES} ${FUSE_LIBRARIES} imss_posix_api imss_shared dl)
