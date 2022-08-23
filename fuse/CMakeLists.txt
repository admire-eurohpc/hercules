find_package(FUSE)

add_library(imss_posix_api STATIC imss_posix_api.c map.cpp mapprefetch.cpp)

add_executable(imssfs
    map.cpp
    imssfs.c
    imss_posix_api.c
    imss_fuse_api.c
    mapprefetch.cpp
    )

target_compile_features(imssfs PRIVATE cxx_std_17)

target_link_directories(imssfs PUBLIC /opt/anaconda3/lib ${UCX_LIBRARIES})
set(CMAKE_C_FLAGS " -fPIC -D_FILE_OFFSET_BITS=64 -lulockmgr -DFUSE")

target_include_directories(imssfs PRIVATE ${FUSE_INCLUDE_DIR})
target_link_libraries(imssfs  ${CMAKE_THREAD_LIBS_INIT} ${FUSE_LIBRARIES}  imss_static )
#target_link_libraries(imssfs  ${CMAKE_THREAD_LIBS_INIT} ${FUSE_LIBRARIES}  imss_static  tcmalloc)

