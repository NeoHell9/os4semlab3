#define _CRT_SECURE_NO_WARNINGS

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <windows.h>
#include <process.h>
#include <time.h>
#include <ctype.h>

#define MIN_PARALLEL_SIZE 1000 // Минимальный размер подмассива для параллельной обработки

typedef struct {
    int* arr;
    int start;
    int end;
} Task;

typedef struct {
    Task* tasks;
    int capacity;
    int size;
    int head;
    int tail;
    HANDLE mutex;
    HANDLE semaphore;
} TaskQueue;

typedef struct {
    TaskQueue* queue;
    int* active_threads;
    HANDLE active_threads_mutex;
} ThreadData;

void Swap(int* arr, int i, int j)
{
    int temp = arr[i];
    arr[i] = arr[j];
    arr[j] = temp;
}

void QuickSort(int* arr, int start, int end)
{
    if (start >= end) return;

    int pivot = arr[(start + end) / 2];
    int i = start;
    int j = end;

    while (i <= j) {
        while (arr[i] < pivot) i++;
        while (arr[j] > pivot) j--;
        if (i <= j) {
            Swap(arr, i, j);
            i++;
            j--;
        }
    }

    QuickSort(arr, start, j);
    QuickSort(arr, i, end);
}

void TaskQueueInit(TaskQueue* queue, int capacity)
{
    queue->tasks = (Task*)malloc(capacity * sizeof(Task));
    queue->capacity = capacity;
    queue->size = 0;
    queue->head = 0;
    queue->tail = 0;
    queue->mutex = CreateMutex(NULL, FALSE, NULL);
    queue->semaphore = CreateSemaphore(NULL, 0, capacity, NULL);
}

void TaskQueuePush(TaskQueue* queue, Task task) 
{
    WaitForSingleObject(queue->mutex, INFINITE);

    queue->tasks[queue->tail] = task;
    queue->tail = (queue->tail + 1) % queue->capacity;
    queue->size++;

    ReleaseMutex(queue->mutex);
    ReleaseSemaphore(queue->semaphore, 1, NULL);
}

int TaskQueuePop(TaskQueue* queue, Task* task) 
{
    WaitForSingleObject(queue->semaphore, INFINITE);
    WaitForSingleObject(queue->mutex, INFINITE);

    if (queue->size == 0) 
    {
        ReleaseMutex(queue->mutex);
        return 0;
    }

    *task = queue->tasks[queue->head];
    queue->head = (queue->head + 1) % queue->capacity;
    queue->size--;

    ReleaseMutex(queue->mutex);
    return 1;
}

void TaskQueueDestroy(TaskQueue* queue) 
{
    free(queue->tasks);
    CloseHandle(queue->mutex);
    CloseHandle(queue->semaphore);
}

unsigned __stdcall WorkerThread(void* param)
{
    ThreadData* data = (ThreadData*)param;
    TaskQueue* queue = data->queue;
    Task task;

    while (1) 
    {
        if (!TaskQueuePop(queue, &task))
        {
            break;
        }

        int start = task.start;
        int end = task.end;
        int* arr = task.arr;

        if (start >= end) 
        {
            continue;
        }

        if (end - start + 1 < MIN_PARALLEL_SIZE)
        {
            QuickSort(arr, start, end);
            continue;
        }

        int pivot = arr[(start + end) / 2];
        int i = start;
        int j = end;

        while (i <= j) 
        {
            while (arr[i] < pivot) i++;
            while (arr[j] > pivot) j--;
            if (i <= j) 
            {
                Swap(arr, i, j);
                i++;
                j--;
            }
        }

        if (start < j) {
            Task new_task = { arr, start, j };
            TaskQueuePush(queue, new_task);
        }

        if (i < end) {
            Task new_task = { arr, i, end };
            TaskQueuePush(queue, new_task);
        }
    }

    // Уменьшаем счетчик активных потоков
    WaitForSingleObject(data->active_threads_mutex, INFINITE);
    (*data->active_threads)--;
    ReleaseMutex(data->active_threads_mutex);

    return 0;
}

int GetNum(FILE* file) 
{
    char currchar = fgetc(file);
    char tempforint[12] = { '\0' };
    int i = 0;

    while (isdigit(currchar)) 
    {
        tempforint[i++] = currchar;
        currchar = fgetc(file);
    }

    return atoi(tempforint);
}

void GetFromFile(FILE* file, int* arr, int n) 
{
    for (int i = 0; i < n; i++) 
    {
        fscanf(file, "%d", &arr[i]);
    }
}

void OutputPrint(int* arr, int numthreads, int n, FILE* file) 
{
    fprintf(file, "%d\n", numthreads);
    fprintf(file, "%d\n", n);

    for (int i = 0; i < n - 1; i++)
    {
        fprintf(file, "%d ", arr[i]);
    }
    fprintf(file, "%d", arr[n - 1]);
}

void ParallelQuickSort(int* arr, int n, int num_threads) 
{
    TaskQueue queue;
    TaskQueueInit(&queue, n/MIN_PARALLEL_SIZE); 

    int active_threads = num_threads;
    HANDLE active_threads_mutex = CreateMutex(NULL, FALSE, NULL);

    ThreadData thread_data = { &queue, &active_threads, active_threads_mutex };

    // Создаем рабочие потоки
    HANDLE* threads = (HANDLE*)malloc(num_threads * sizeof(HANDLE));
    for (int i = 0; i < num_threads; i++) {
        threads[i] = (HANDLE)_beginthreadex(NULL, 0, WorkerThread, &thread_data, 0, NULL);
    }

    // Добавляем начальную задачу
    Task initial_task = { arr, 0, n - 1 };
    TaskQueuePush(&queue, initial_task);

    // Ждем завершения всех задач
    while (1) {
        WaitForSingleObject(active_threads_mutex, INFINITE);
        if (active_threads == 0) {
            ReleaseMutex(active_threads_mutex);
            break;
        }
        ReleaseMutex(active_threads_mutex);
        Sleep(10);
    }

    // Закрываем потоки
    for (int i = 0; i < num_threads; i++) {
        CloseHandle(threads[i]);
    }
    free(threads);
    CloseHandle(active_threads_mutex);
    TaskQueueDestroy(&queue);
}

int main() {
    char inputname[] = "input.txt";
    char outputname[] = "output.txt";
    char timename[] = "time.txt";

    FILE* f_input = fopen(inputname, "r");
    if (!f_input) {
        perror("Failed to open input file");
        return 1;
    }

    int num_threads = GetNum(f_input);
    int n = GetNum(f_input);

    int* arr = (int*)malloc(n * sizeof(int));
    if (!arr) 
    {
        perror("Memory allocation failed");
        fclose(f_input);
        return 1;
    }

    GetFromFile(f_input, arr, n);
    fclose(f_input);

    // Замер времени только сортировки
    clock_t start = clock();
    if (num_threads == 1 || n < MIN_PARALLEL_SIZE) 
    {
        QuickSort(arr, 0, n - 1);
    }
    else 
    {
        ParallelQuickSort(arr, n, num_threads);
    }
    clock_t end = clock();
    double time_spent = (double)(end - start)*CLOCKS_PER_SEC/1000;

    FILE* f_output = fopen(outputname, "w");
    if (!f_output)
    {
        perror("Failed to open output file");
        free(arr);
        return 1;
    }

    OutputPrint(arr, num_threads, n, f_output);
    fclose(f_output);

    FILE* f_time = fopen(timename, "w");
    if (!f_time)
    {
        perror("Failed to open time file");
        free(arr);
        return 1;
    }

    fprintf(f_time, "%.0f", time_spent);
    fclose(f_time);

    free(arr);
    return 0;
}