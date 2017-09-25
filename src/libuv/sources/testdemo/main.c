#include <stdio.h>
#include <stdlib.h>
#include <tchar.h>
#include <uv.h>

int main(int argc, char ** argv)
{
    uv_loop_t *loop = uv_loop_new();
    printf("Now quitting.\n");
    uv_run(loop, UV_RUN_DEFAULT);

    return 0;
}
