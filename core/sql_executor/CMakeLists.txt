include_directories(analyze)
include_directories(execution)
include_directories(optimizer)
include_directories(parser)

set(SQL_RUN_SRC 
        analyze/analyze.cc
        optimizer/planner.cc
        parser/ast.cc
        parser/lex.yy.cpp
        parser/yacc.tab.cpp
        execution/ExecutionManager.cc
)

add_library(sql STATIC ${SQL_RUN_SRC})

target_link_libraries(sql core)
