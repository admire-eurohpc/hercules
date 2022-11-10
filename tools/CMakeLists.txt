set_source_files_properties(./mount.c ../src/hercules.c ../src/slog.c PROPERTIES LANGUAGE CXX)

add_executable(mount.imss
    mount.c
    ../src/hercules.c
    ../src/map_ep.cpp
    ../src/slog.c
    )

add_executable(umount.imss
    umount.c
    ../src/map_ep.cpp
    ../src/slog.c
    )

set(CMAKE_C_FLAGS "-fPIC")
set(CMAKE_CXX_FLAGS "-fPIC")

add_library(imss_posix_api STATIC ../fuse/imss_posix_api.c ../fuse/map.cpp ../fuse/mapprefetch.cpp ../src/slog.c)

add_library (imss_posix SHARED imss_posix.c mapfd.cpp ../fuse/map.cpp ../src/slog.c) 

target_include_directories(imss_posix PRIVATE ../fuse  .)

target_link_directories(imss_posix PUBLIC ${UCX_LIBRARIES})
target_link_directories(mount.imss PUBLIC ${UCX_LIBRARIES})
target_link_directories(umount.imss PUBLIC ${UCX_LIBRARIES})

target_link_libraries(mount.imss  ${CMAKE_THREAD_LIBS_INIT} ${GLIB_LIBRARIES} imss_static)
target_link_libraries(umount.imss ${CMAKE_THREAD_LIBS_INIT} ${GLIB_LIBRARIES} imss_static)
target_link_libraries(imss_posix  ${CMAKE_THREAD_LIBS_INIT} ${GLIB_LIBRARIES} imss_posix_api imss_shared dl)
