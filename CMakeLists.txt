#设置cmake的版本和项目名称
cmake_minimum_required(VERSION 3.22)
project(KVStorage)
set(CMAKE_CXX_STANDARD 20)
# 生成debug版本，可以进行gdb调试
set(CMAKE_BUILD_TYPE "Debug")

# 设置项目可执行文件输出的路径
set(EXECUTABLE_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/bin)
# 设置项目库文件输出的路径
set(LIBRARY_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/lib)

# 设置项目编译头文件搜索路径
include_directories(${PROJECT_SOURCE_DIR}/src/common/include)
include_directories(${PROJECT_SOURCE_DIR}/src/raftCore/include)
include_directories(${PROJECT_SOURCE_DIR}/src/raftRpcPro/include)
# 设置项目库文件搜索路径 -L
link_directories(${PROJECT_SOURCE_DIR}/lib)
# src包含了所有的相关代码
add_subdirectory(src)
# example包含了使用的示例代码
#add_subdirectory(example)

add_library(skip_list_on_raft STATIC ${src_common} ${src_raftCore} ${src_raftRpcPro})
#target_link_libraries()
