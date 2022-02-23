find_package(FUSE REQUIRED)


add_library(imss_posix_api STATIC imss_posix_api.c map.cpp)

add_executable(imssfs
    map.cpp
    imssfs.c
	imss_posix_api.c
    )


set(CMAKE_C_FLAGS "-D_FILE_OFFSET_BITS=64 -lulockmgr -DFUSE")
#set(CMAKE_C_LDFLAGS "-L/opt/anaconda3/lib")



target_include_directories(imssfs PRIVATE ${FUSE_INCLUDE_DIR})
target_link_libraries(imssfs  ${CMAKE_THREAD_LIBS_INIT} zmq  ${FUSE_LIBRARIES}  imss_static  tcmalloc)

target_compile_features(imssfs PRIVATE cxx_std_17)