#define _CRT_SECURE_NO_WARNINGS

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <windows.h>
#include <process.h>
#include <time.h>
#include <ctype.h>


#define SIZE 20
#define LENARRAY 100000000
#define FINDLENGHT(arr) (int)(sizeof(arr)/(int)sizeof(arr[0]))
#define FILENAMELEN 100
#define LENINT 11
#define LEN 100

void Swap(int* arr, int i, int j)
{
	int temp = 0;

	temp = arr[i];
	arr[i] = arr[j];
	arr[j] = temp;
}

void QuickSort(int* arr, int start, int end)
{
	if (start >= end) return;

	int x = arr[(start + end) / 2];

	int temp = 0;
	int i = start;
	int j = end;
	while (i <= j)
	{

		while (arr[i] < x)
		{
			i++;
		}
		while (arr[j] > x)
		{
			j--;
		}
		if (i <= j)
		{
			Swap(arr, i, j);
			i++;
			j--;
		}
	}

	if (i < end)
	{
		QuickSort(arr, i, end);
	}
	if (j > start)
	{
		QuickSort(arr, start, j);
	}
}

int GetNum(FILE* file)
{
	char currchar = fgetc(file);
	char tempforint[LENINT] = { '\0' };
	
	int i = 0;

	while (isdigit(currchar))
	{
		tempforint[i++] = currchar;
		currchar = fgetc(file);
	}

	int temp = atoi(tempforint);

	return temp;
}

void GetFromFile(FILE* file, int* arr)
{
	char currchar = fgetc(file);
	char tempforint[LENINT] = { '\0' };
	int i = 0;
	int j = 0;

	while (!feof(file))
	{
		while (isdigit(currchar))
		{
			tempforint[i++] = currchar;
			currchar = fgetc(file);
		}
		i = 0;
		int temp = atoi(tempforint);
		memset(tempforint, '\0', sizeof(tempforint));
		arr[j++] = temp;
		if (currchar == ' ')
		{
			currchar = fgetc(file);
		}
	}
	//return j;
}

void OutputPrint(int* arr, int numthreads, int n, FILE* file)
{
	fprintf(file, "%d\n", numthreads);
	fprintf(file, "%d\n", n);

	int i = 0;
	for (; i < n - 1; i++)
	{
		fprintf(file, "%d ", arr[i]);
	}
	fprintf(file, "%d", arr[i]);
}

int main()
{
	char inputname[52] = "input.txt";
	char outputname[52] = "output.txt";

	FILE* f_input = fopen(inputname, "r");
	FILE* f_output = fopen(outputname, "w");

	int numthreads = GetNum(f_input);
	int n = GetNum(f_input);

	int* arr = (int*)calloc(n, sizeof(int));
	
	GetFromFile(f_input, arr);
	QuickSort(arr, 0, n-1);

	OutputPrint(arr, numthreads, n, f_output);

	return 0;
}