#include "util/message.h"

char* next_token(char** tokenizer, message_status* status);

char* trim_newline(char *str);
char* trim_parenthesis(char *str);
char* trim_whitespace(char *str);
char* trim_quotes(char *str);
