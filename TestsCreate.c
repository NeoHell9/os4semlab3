#define _CRT_SECURE_NO_WARNINGS

#include <stdio.h>
#include <stdlib.h>

#define N 10000000

int main()
{
	FILE* fRes;
	fRes = fopen("d.txt", "w");

	for (int i = 1; i < N; i++)
	{
		fprintf(fRes, "%d ", rand());
	}
	fprintf(fRes, "%d", rand());

	return 0;
}