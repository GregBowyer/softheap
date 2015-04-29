#include "common.h"

void debugprintf(char *file, int line, const char *format, ...)
{
    char *file_details = NULL;
    int len = asprintf(&file_details, "[%s:%d] ", file, line);
    if (len != -1) {
        char *new_fmt = (char*) calloc(sizeof(char), len + strlen(format) + 1);
        strcpy(new_fmt, file_details);
        free(file_details);
        strcpy((char*) &new_fmt[len], format);

        va_list ap;
        va_start(ap, format);
        vprintf(new_fmt, ap);
        va_end(ap);

        free(new_fmt);
    }
}
