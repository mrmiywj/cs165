#include <unistd.h>
#include <stdio.h>

int main(void) {
    printf("%i\n", getpagesize());
    printf("%i\n", sysconf(_SC_PAGESIZE));
}
