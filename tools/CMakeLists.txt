# set_source_files_properties(./mount.c ../src/hercules.c ../src/slog.c PROPERTIES LANGUAGE CXX)
set_source_files_properties(../src/hercules.c ../src/slog.c PROPERTIES LANGUAGE CXX)


# add_executable(mount.hercules
#     mount.c
#     ../src/hercules.c
#     ../src/map_ep.cpp
#     ../src/slog.c
#     )

# add_executable(umount.hercules
#     umount.c
#     ../src/map_ep.cpp
#     ../src/slog.c
#     )

set(CMAKE_C_FLAGS "-fPIC")
set(CMAKE_CXX_FLAGS "-fPIC")

# set(CMAKE_BUILD_TYPE Debug)
# set(CMAKE_BUILD_TYPE RelWithDebInfo)

add_library(hercules_posix_api STATIC ../fuse/imss_posix_api.c ../fuse/map.cpp ../fuse/mapprefetch.cpp ../src/slog.c)

add_library (hercules_posix SHARED imss_posix.c mapfd.cpp flags.c resolvepath.c ../fuse/map.cpp ../src/slog.c ../src/cfg_parse.c ) 

target_include_directories(hercules_posix PRIVATE ../fuse  .)

target_link_directories(hercules_posix PUBLIC ${UCX_LIBRARIES})
# target_link_directories(mount.hercules PUBLIC ${UCX_LIBRARIES})
# target_link_directories(umount.hercules PUBLIC ${UCX_LIBRARIES})

# target_link_libraries(mount.hercules  ${CMAKE_THREAD_LIBS_INIT} ${GLIB_LIBRARIES} imss_static)
# target_link_libraries(umount.hercules ${CMAKE_THREAD_LIBS_INIT} ${GLIB_LIBRARIES} imss_static)
target_link_libraries(hercules_posix ${CMAKE_THREAD_LIBS_INIT} ${GLIB_LIBRARIES} hercules_posix_api imss_shared dl)


install(TARGETS hercules_posix DESTINATION lib)
install(TARGETS hercules_posix_api DESTINATION lib)

# install(TARGETS mount.hercules DESTINATION bin)
# install(TARGETS umount.hercules DESTINATION bin)
