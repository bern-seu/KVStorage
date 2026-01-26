#设置cmake的版本和项目名称
cmake_minimum_required(VERSION 3.22)
project(KVStorage)
set(CMAKE_CXX_STANDARD 20)
# ----------------- [新增开始] TSan 开关配置 -----------------
# 定义一个开关 ENABLE_TSAN，默认是 OFF (关闭)
option(ENABLE_TSAN "Enable ThreadSanitizer" OFF)

if(ENABLE_TSAN)
    message(STATUS "-------------- ThreadSanitizer ENABLED --------------")
    # 1. 编译选项：所有源文件都会带上 -fsanitize=thread
    # -g 用于显示行号，-O1 优化能让 TSan 跑得快一点且不影响调试
    add_compile_options(-fsanitize=thread -g -O1)
    
    # 2. 链接选项：确保链接器也能链接 TSan 库
    # 注意：这对于生成可执行文件至关重要，否则会报 reference 错误
    add_link_options(-fsanitize=thread)
endif()
# ----------------- [新增结束] -----------------
# 生成debug版本，可以进行gdb调试
set(CMAKE_BUILD_TYPE "Debug")

# 设置项目可执行文件输出的路径
set(EXECUTABLE_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/bin)
# 设置项目库文件输出的路径
set(LIBRARY_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/lib)

# 设置项目编译头文件搜索路径
include_directories(${PROJECT_SOURCE_DIR}/example)
include_directories(${PROJECT_SOURCE_DIR}/src/common/include)
include_directories(${PROJECT_SOURCE_DIR}/src/fiber/include)
include_directories(${PROJECT_SOURCE_DIR}/src/raftCore/include)
include_directories(${PROJECT_SOURCE_DIR}/src/raftRpcPro/include)
include_directories(${PROJECT_SOURCE_DIR}/src/rpc/include)
include_directories(${PROJECT_SOURCE_DIR}/src/skipList/include)
include_directories(${PROJECT_SOURCE_DIR}/src/raftClerk/include)
# 设置项目库文件搜索路径 -L
link_directories(${PROJECT_SOURCE_DIR}/lib)
# src包含了所有的相关代码
add_subdirectory(src)
#example包含了使用的示例代码
add_subdirectory(example)

add_library(skip_list_on_raft STATIC ${src_common} ${src_fiber} ${src_raftCore} ${src_raftRpcPro} ${src_rpc} ${rpc_example})
target_link_libraries(skip_list_on_raft muduo_net muduo_base pthread dl)