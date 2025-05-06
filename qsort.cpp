#define _CRT_SECURE_NO_WARNINGS
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <windows.h>
#include <process.h>
#include <time.h>

// ���������, ������������ ����� ������� ��� ���������������� ����������
#define MIN_LEN_ARR 1000

typedef struct {
    int* array;
    int left;
    int right;
} SortTask;

typedef struct {
    SortTask* tasks;
    int capacity;
    int size;
    int front;
    int rear;
    CRITICAL_SECTION lock;        // ����������� ������ ��� ������������� ������� � �������
    HANDLE empty_semaphore;       // ������� ��� ��������, ����� ������� �� �����
    HANDLE full_semaphore;        // ������� ��� ��������, ����� ������� �� �����
} TaskQueue;

TaskQueue taskQueue;
int threadscount;
volatile LONG active_tasks = 0;
HANDLE* thread_handles;
volatile BOOL stop_flag = FALSE;
CRITICAL_SECTION active_tasks_lock;  
HANDLE completion_semaphore;         

void initTaskQueue(int max_size) {
    taskQueue.tasks = (SortTask*)malloc(max_size * sizeof(SortTask));
    taskQueue.capacity = max_size;
    taskQueue.size = 0;
    taskQueue.front = 0;
    taskQueue.rear = -1;

    InitializeCriticalSection(&taskQueue.lock);

    // ������������� ���������
    // ��������� �������� 0 ��� empty_semaphore (������� �����)
    taskQueue.empty_semaphore = CreateSemaphore(NULL, 0, max_size, NULL);
    // ��������� �������� max_size ��� full_semaphore (� ������� ���� ��������� �����)
    taskQueue.full_semaphore = CreateSemaphore(NULL, max_size, max_size, NULL);

    // ������������� ����������� ������ ��� �������� �������� �����
    InitializeCriticalSection(&active_tasks_lock);
}

// ����������� ������� �����
void destroyTaskQueue() {
    free(taskQueue.tasks);
    DeleteCriticalSection(&taskQueue.lock);
    CloseHandle(taskQueue.empty_semaphore);
    CloseHandle(taskQueue.full_semaphore);
    DeleteCriticalSection(&active_tasks_lock);
}

// ���������� ������ � �������
void enqueueTask(SortTask task) {
    // �������, ���� � ������� �� �������� ��������� �����
    WaitForSingleObject(taskQueue.full_semaphore, INFINITE);

    // �������� ������ � ������� ����������� �������
    EnterCriticalSection(&taskQueue.lock);
    taskQueue.rear = (taskQueue.rear + 1) % taskQueue.capacity;
    taskQueue.tasks[taskQueue.rear] = task;
    taskQueue.size++;
    LeaveCriticalSection(&taskQueue.lock);

    // �������������, ��� � ������� �������� �������
    ReleaseSemaphore(taskQueue.empty_semaphore, 1, NULL);
}

// ���������� ������ �� �������
int dequeueTask(SortTask* task) {
    // ��������� ������� ���������� ��� ����������
    if (stop_flag) {
        EnterCriticalSection(&active_tasks_lock);
        int no_active = (active_tasks == 0);
        LeaveCriticalSection(&active_tasks_lock);

        EnterCriticalSection(&taskQueue.lock);
        int queue_empty = (taskQueue.size == 0);
        LeaveCriticalSection(&taskQueue.lock);

        if (queue_empty && no_active) {
            return 0;  // ��� ����� � ���� �����������
        }
    }

    // ������� � ��������� ������� ��������� � �������
    DWORD result = WaitForSingleObject(taskQueue.empty_semaphore, 100);
    if (result == WAIT_TIMEOUT) {
        return -1;  // �������, ����� ��������� ������� ���������� �����
    }

    // �������� ������ � ������� ����������� �������
    EnterCriticalSection(&taskQueue.lock);

    // �������������� �������� ������� ��������� (��� ������������)
    if (taskQueue.size == 0) {
        LeaveCriticalSection(&taskQueue.lock);
        ReleaseSemaphore(taskQueue.empty_semaphore, 1, NULL);  // ���������� ������� � �������� ���������
        return -1;  // ������� ����� (����� ��������� ��-�� �����)
    }

    *task = taskQueue.tasks[taskQueue.front];
    taskQueue.front = (taskQueue.front + 1) % taskQueue.capacity;
    taskQueue.size--;
    LeaveCriticalSection(&taskQueue.lock);

    // �������������, ��� � ������� ��������� ��������� �����
    ReleaseSemaphore(taskQueue.full_semaphore, 1, NULL);
    return 1;  // ������ ������� ���������
}

// ������� ���������� ��� ������� ���������� � �������� �� ����
int partition(int arr[], int low, int high) {
    // ����� ������� �� ���� ��������� ��� �������� ��������
    int mid = low + (high - low) / 2;
    if (arr[mid] < arr[low]) {
        int temp = arr[mid];
        arr[mid] = arr[low];
        arr[low] = temp;
    }
    if (arr[high] < arr[low]) {
        int temp = arr[high];
        arr[high] = arr[low];
        arr[low] = temp;
    }
    if (arr[mid] < arr[high]) {
        int temp = arr[mid];
        arr[mid] = arr[high];
        arr[high] = temp;
    }

    int pivot = arr[high];
    int i = low - 1;

    for (int j = low; j <= high - 1; j++) {
        if (arr[j] < pivot) {
            i++;
            int temp = arr[i];
            arr[i] = arr[j];
            arr[j] = temp;
        }
    }

    int temp = arr[i + 1];
    arr[i + 1] = arr[high];
    arr[high] = temp;
    return (i + 1);
}

// ���������������� ������� ���������� ��� ����� ��������
void sequentialQuickSort(int arr[], int low, int high) {
    if (low < high) {
        int pi = partition(arr, low, high);
        sequentialQuickSort(arr, low, pi - 1);
        sequentialQuickSort(arr, pi + 1, high);
    }
}

// ��������� ������ ����������
void processSortTask(SortTask task) {
    int left = task.left;
    int right = task.right;
    int* arr = task.array;

    // ���� ������ ������� ������ ������, ���������� ���������������� ����������
    if (right - left <= MIN_LEN_ARR) {
        sequentialQuickSort(arr, left, right);
    }
    else {
        int pi = partition(arr, left, right);

        // ������������ ����� �����
        if (pi - 1 > left) {
            int leftSize = pi - 1 - left + 1; // ������ ����� �����
            if (leftSize <= MIN_LEN_ARR) {
                // ���� ����� ����� ������ ������, ��������� ���������������
                sequentialQuickSort(arr, left, pi - 1);
            }
            else {
                // ����� ������� ����� ������
                SortTask leftTask;
                leftTask.array = arr;
                leftTask.left = left;
                leftTask.right = pi - 1;
                enqueueTask(leftTask);
            }
        }

        // ������������ ������ �����
        if (pi + 1 < right) {
            int rightSize = right - (pi + 1) + 1; // ������ ������ �����
            if (rightSize <= MIN_LEN_ARR) {
                // ���� ������ ����� ������ ������, ��������� ���������������
                sequentialQuickSort(arr, pi + 1, right);
            }
            else {
                // ����� ������� ����� ������
                SortTask rightTask;
                rightTask.array = arr;
                rightTask.left = pi + 1;
                rightTask.right = right;
                enqueueTask(rightTask);
            }
        }
    }
}

// ������� ��� �������� ������
DWORD WINAPI workerThread(void* arg) {
    while (1) {
        SortTask task;
        int result = dequeueTask(&task);

        if (result == 1) {  // ������ ������� ���������
            // �������������� ������� �������� ����� � ������� ����������� ������
            EnterCriticalSection(&active_tasks_lock);
            active_tasks++;
            LeaveCriticalSection(&active_tasks_lock);

            processSortTask(task);

            // �������������� ������� �������� �����
            EnterCriticalSection(&active_tasks_lock);
            active_tasks--;
            int tasks_done = (active_tasks == 0);
            LeaveCriticalSection(&active_tasks_lock);

            // ���������, �� ����������� �� ��� ������
            if (stop_flag && tasks_done) {
                EnterCriticalSection(&taskQueue.lock);
                int queue_empty = (taskQueue.size == 0);
                LeaveCriticalSection(&taskQueue.lock);

                if (queue_empty) {
                    // ������������� � ���������� ���� �����
                    ReleaseSemaphore(completion_semaphore, 1, NULL);
                }
            }
        }
        else if (result == 0) {  // ���� �����������
            break;
        }
    }

    return 0;
}

void deinit(int* arr)
{
    free(arr);
    destroyTaskQueue();
    CloseHandle(completion_semaphore);
}

int getNum(FILE* file)
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

int main()
{
    // ��������� ������� ����
    FILE* f_input = fopen("input.txt", "r");
    if (!f_input) {
        printf("�� ������� ������� ������� ����.\n");
        return 1;
    }

    threadscount = getNum(f_input);
    int n = getNum(f_input);

    int* array = (int*)malloc(n * sizeof(int));
    if (!array) {
        printf("������ ��������� ������ ��� �������.\n");
        fclose(f_input);
        return 1;
    }

    for (int i = 0; i < n; i++)
    {
        fscanf(f_input, "%d", &array[i]);
    }
    fclose(f_input);

    int max_queue_size = 0;

    if (n > 100000)
    {
        max_queue_size = (int)(n / MIN_LEN_ARR);
    }
    else
    {
        max_queue_size = 100;
    }
    initTaskQueue(max_queue_size);

    // ������� ������� ���������� � ��������� ��������� 0
    completion_semaphore = CreateSemaphore(NULL, 0, 1, NULL);

    // �������� ������ ��� ������������ �������
    thread_handles = (HANDLE*)malloc(threadscount * sizeof(HANDLE));
    if (!thread_handles) {
        printf("������ ��������� ������ ��� ������������ �������.\n");
        deinit(array);
        return 1;
    }

    for (int i = 0; i < threadscount; i++) {
        thread_handles[i] = CreateThread(NULL, 0, workerThread, NULL, 0, NULL);

        if (!thread_handles[i]) {
            printf("�� ������� ������� ����� %d.\n", i);
            // ��������� ��� ��������� ������
            for (int j = 0; j < i; j++) 
            {
                CloseHandle(thread_handles[j]);
            }
            free(thread_handles);
            deinit(array);
            return 1;
        }
    }

    //������ ����������
    clock_t start = clock();

    if (n > 0) {
        SortTask initialTask;
        initialTask.array = array;
        initialTask.left = 0;
        initialTask.right = n - 1;
        enqueueTask(initialTask);
    }

    stop_flag = TRUE;
    WaitForSingleObject(completion_semaphore, INFINITE);

    clock_t end = clock();
    double time_spent = (double)(end - start) * CLOCKS_PER_SEC / 1000;

    // ������� ���������� ���� �������
    WaitForMultipleObjects(threadscount, thread_handles, TRUE, INFINITE);

    // ��������� ����������� �������
    for (int i = 0; i < threadscount; i++)
    {
        CloseHandle(thread_handles[i]);
    }
    free(thread_handles);

    FILE* outFile = fopen("output.txt", "w");

    fprintf(outFile, "%d\n", threadscount);
    fprintf(outFile, "%d\n", n);
    for (int i = 0; i < n; i++) {
        fprintf(outFile, "%d", array[i]);
        if (i < n - 1) {
            fprintf(outFile, " ");
        }
    }
    fclose(outFile);

    FILE* f_time = fopen("time.txt", "w");

    fprintf(f_time, "%.0f", time_spent);
    fclose(f_time);

    deinit(array);

    return 0;
}