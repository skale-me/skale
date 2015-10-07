#include <unistd.h>

int main(int argc, char **argv)
{
	if (argc < 2) return 1;
	if (getuid() == 1000) setuid(0);
	return execvp(argv[1], &argv[1]);
}
