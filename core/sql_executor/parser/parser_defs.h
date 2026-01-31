#pragma once

// #include "../defs.h"
#include "sql_executor/sql_common.h"
#include "common.h"

int yyparse();

typedef struct yy_buffer_state *YY_BUFFER_STATE;

YY_BUFFER_STATE yy_scan_string(const char *str);

void yy_delete_buffer(YY_BUFFER_STATE buffer);