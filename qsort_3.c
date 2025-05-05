#define _CRT_SECURE_NO_WARNINGS
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <windows.h>
#include <process.h>
#include <time.h>

// Константа, определяющая порог размера для последовательной сортировки
#define SEQUENTIAL_THRESHOLD 1000

// Структура для задачи сортировки
typedef struct {
    int* array;
    int left;
    int right;
} SortTask;

// Структура для очереди задач
typedef struct {
    SortTask* tasks;
    int capacity;
    int size;
    int front;
    int rear;
    CRITICAL_SECTION lock;        // Критическая секция для синхронизации доступа к очереди
    HANDLE empty_semaphore;       // Семафор для ожидания, когда очередь не пуста
    HANDLE full_semaphore;        // Семафор для ожидания, когда очередь не полна
} TaskQueue;

// Глобальные переменные
TaskQueue taskQueue;
int numThreads;
volatile LONG active_tasks = 0;
HANDLE* thread_handles;
volatile BOOL stop_flag = FALSE;
CRITICAL_SECTION active_tasks_lock;  // Критическая секция для активных задач
HANDLE completion_semaphore;         // Семафор для сигнализации о завершении всех задач

// Инициализация очереди задач
void initTaskQueue(int max_size) {
    taskQueue.tasks = (SortTask*)malloc(max_size * sizeof(SortTask));
    taskQueue.capacity = max_size;
    taskQueue.size = 0;
    taskQueue.front = 0;
    taskQueue.rear = -1;

    // Инициализация критической секции
    InitializeCriticalSection(&taskQueue.lock);

    // Инициализация семафоров
    // Начальное значение 0 для empty_semaphore (очередь пуста)
    taskQueue.empty_semaphore = CreateSemaphore(NULL, 0, max_size, NULL);
    // Начальное значение max_size для full_semaphore (в очереди есть свободное место)
    taskQueue.full_semaphore = CreateSemaphore(NULL, max_size, max_size, NULL);

    // Инициализация критической секции для счетчика активных задач
    InitializeCriticalSection(&active_tasks_lock);
}

// Уничтожение очереди задач
void destroyTaskQueue() {
    free(taskQueue.tasks);
    DeleteCriticalSection(&taskQueue.lock);
    CloseHandle(taskQueue.empty_semaphore);
    CloseHandle(taskQueue.full_semaphore);
    DeleteCriticalSection(&active_tasks_lock);
}

// Добавление задачи в очередь
void enqueueTask(SortTask task) {
    // Ожидаем, пока в очереди не появится свободное место
    WaitForSingleObject(taskQueue.full_semaphore, INFINITE);

    // Защищаем доступ к очереди критической секцией
    EnterCriticalSection(&taskQueue.lock);
    taskQueue.rear = (taskQueue.rear + 1) % taskQueue.capacity;
    taskQueue.tasks[taskQueue.rear] = task;
    taskQueue.size++;
    LeaveCriticalSection(&taskQueue.lock);

    // Сигнализируем, что в очереди появился элемент
    ReleaseSemaphore(taskQueue.empty_semaphore, 1, NULL);
}

// Извлечение задачи из очереди
int dequeueTask(SortTask* task) {
    // Проверяем условие завершения без блокировки
    if (stop_flag) {
        EnterCriticalSection(&active_tasks_lock);
        int no_active = (active_tasks == 0);
        LeaveCriticalSection(&active_tasks_lock);

        EnterCriticalSection(&taskQueue.lock);
        int queue_empty = (taskQueue.size == 0);
        LeaveCriticalSection(&taskQueue.lock);

        if (queue_empty && no_active) {
            return 0;  // Нет задач и пора заканчивать
        }
    }

    // Ожидаем с таймаутом наличие элементов в очереди
    DWORD result = WaitForSingleObject(taskQueue.empty_semaphore, 100);
    if (result == WAIT_TIMEOUT) {
        return -1;  // Таймаут, нужно проверить условия завершения снова
    }

    // Защищаем доступ к очереди критической секцией
    EnterCriticalSection(&taskQueue.lock);

    // Дополнительная проверка наличия элементов (для безопасности)
    if (taskQueue.size == 0) {
        LeaveCriticalSection(&taskQueue.lock);
        ReleaseSemaphore(taskQueue.empty_semaphore, 1, NULL);  // Возвращаем семафор в исходное состояние
        return -1;  // Очередь пуста (может произойти из-за гонки)
    }

    *task = taskQueue.tasks[taskQueue.front];
    taskQueue.front = (taskQueue.front + 1) % taskQueue.capacity;
    taskQueue.size--;
    LeaveCriticalSection(&taskQueue.lock);

    // Сигнализируем, что в очереди появилось свободное место
    ReleaseSemaphore(taskQueue.full_semaphore, 1, NULL);
    return 1;  // Задача успешно извлечена
}

// Функция разделения для быстрой сортировки с медианой из трех
int partition(int arr[], int low, int high) {
    // Выбор медианы из трех элементов как опорного элемента
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

// Последовательная быстрая сортировка для малых массивов
void sequentialQuickSort(int arr[], int low, int high) {
    if (low < high) {
        int pi = partition(arr, low, high);
        sequentialQuickSort(arr, low, pi - 1);
        sequentialQuickSort(arr, pi + 1, high);
    }
}

// Обработка задачи сортировки
void processSortTask(SortTask task) {
    int left = task.left;
    int right = task.right;
    int* arr = task.array;

    // Если размер массива меньше порога, используем последовательную сортировку
    if (right - left <= SEQUENTIAL_THRESHOLD) {
        sequentialQuickSort(arr, left, right);
    }
    else {
        int pi = partition(arr, left, right);

        // Обрабатываем левую часть
        if (pi - 1 > left) {
            int leftSize = pi - 1 - left + 1; // Размер левой части
            if (leftSize <= SEQUENTIAL_THRESHOLD) {
                // Если левая часть меньше порога, сортируем последовательно
                sequentialQuickSort(arr, left, pi - 1);
            }
            else {
                // Иначе создаем новую задачу
                SortTask leftTask;
                leftTask.array = arr;
                leftTask.left = left;
                leftTask.right = pi - 1;
                enqueueTask(leftTask);
            }
        }

        // Обрабатываем правую часть
        if (pi + 1 < right) {
            int rightSize = right - (pi + 1) + 1; // Размер правой части
            if (rightSize <= SEQUENTIAL_THRESHOLD) {
                // Если правая часть меньше порога, сортируем последовательно
                sequentialQuickSort(arr, pi + 1, right);
            }
            else {
                // Иначе создаем новую задачу
                SortTask rightTask;
                rightTask.array = arr;
                rightTask.left = pi + 1;
                rightTask.right = right;
                enqueueTask(rightTask);
            }
        }
    }
}

// Функция для рабочего потока
DWORD WINAPI workerThread(void* arg) {
    while (1) {
        SortTask task;
        int result = dequeueTask(&task);

        if (result == 1) {  // Задача успешно извлечена
            // Инкрементируем счетчик активных задач с помощью критической секции
            EnterCriticalSection(&active_tasks_lock);
            active_tasks++;
            LeaveCriticalSection(&active_tasks_lock);

            processSortTask(task);

            // Декрементируем счетчик активных задач
            EnterCriticalSection(&active_tasks_lock);
            active_tasks--;
            int tasks_done = (active_tasks == 0);
            LeaveCriticalSection(&active_tasks_lock);

            // Проверяем, не закончились ли все задачи
            if (stop_flag && tasks_done) {
                EnterCriticalSection(&taskQueue.lock);
                int queue_empty = (taskQueue.size == 0);
                LeaveCriticalSection(&taskQueue.lock);

                if (queue_empty) {
                    // Сигнализируем о завершении всех задач
                    ReleaseSemaphore(completion_semaphore, 1, NULL);
                }
            }
        }
        else if (result == 0) {  // Пора заканчивать
            break;
        }
        // Для result == -1 просто продолжаем попытки получить задачу
    }

    return 0;
}

int main() {
    // Открываем входной файл
    FILE* inFile = fopen("input.txt", "r");
    if (!inFile) {
        fprintf(stderr, "Не удалось открыть входной файл.\n");
        return 1;
    }

    // Читаем количество потоков и элементов
    fscanf(inFile, "%d", &numThreads);
    int numElements;
    fscanf(inFile, "%d", &numElements);

    // Выделяем память для массива
    int* array = (int*)malloc(numElements * sizeof(int));
    if (!array) {
        fprintf(stderr, "Ошибка выделения памяти для массива.\n");
        fclose(inFile);
        return 1;
    }

    // Читаем элементы массива
    for (int i = 0; i < numElements; i++) {
        fscanf(inFile, "%d", &array[i]);
    }
    fclose(inFile);

    // Инициализируем очередь задач (с запасом для большого числа подзадач)
    int max_queue_size = numElements > 10000 ? 10000 : numElements;
    initTaskQueue(max_queue_size);

    // Создаем семафор завершения с начальным счетчиком 0
    completion_semaphore = CreateSemaphore(NULL, 0, 1, NULL);

    // Выделяем память для дескрипторов потоков
    thread_handles = (HANDLE*)malloc(numThreads * sizeof(HANDLE));
    if (!thread_handles) {
        fprintf(stderr, "Ошибка выделения памяти для дескрипторов потоков.\n");
        free(array);
        destroyTaskQueue();
        CloseHandle(completion_semaphore);
        return 1;
    }

    // Создаем рабочие потоки
    for (int i = 0; i < numThreads; i++) {
        thread_handles[i] = CreateThread(NULL, 0, workerThread, NULL, 0, NULL);

        if (!thread_handles[i]) {
            fprintf(stderr, "Не удалось создать поток %d.\n", i);
            // Закрываем уже созданные потоки
            for (int j = 0; j < i; j++) {
                CloseHandle(thread_handles[j]);
            }
            free(thread_handles);
            free(array);
            destroyTaskQueue();
            CloseHandle(completion_semaphore);
            return 1;
        }
    }

    // Засекаем время начала сортировки
    LARGE_INTEGER frequency, start, end;
    QueryPerformanceFrequency(&frequency);
    QueryPerformanceCounter(&start);

    // Если массив не пустой, добавляем задачу сортировки в очередь
    if (numElements > 0) {
        SortTask initialTask;
        initialTask.array = array;
        initialTask.left = 0;
        initialTask.right = numElements - 1;
        enqueueTask(initialTask);
    }

    // Устанавливаем флаг останова и ожидаем завершения всех задач
    stop_flag = TRUE;
    WaitForSingleObject(completion_semaphore, INFINITE);

    // Засекаем время окончания сортировки
    QueryPerformanceCounter(&end);
    double duration = (double)(end.QuadPart - start.QuadPart) * 1000.0 / frequency.QuadPart;

    // Ожидаем завершения всех потоков
    WaitForMultipleObjects(numThreads, thread_handles, TRUE, INFINITE);

    // Закрываем дескрипторы потоков
    for (int i = 0; i < numThreads; i++) {
        CloseHandle(thread_handles[i]);
    }
    free(thread_handles);

    // Записываем результат сортировки в файл
    FILE* outFile = fopen("output.txt", "w");
    if (!outFile) {
        fprintf(stderr, "Не удалось открыть выходной файл.\n");
        free(array);
        destroyTaskQueue();
        CloseHandle(completion_semaphore);
        return 1;
    }

    fprintf(outFile, "%d\n", numThreads);
    fprintf(outFile, "%d\n", numElements);
    for (int i = 0; i < numElements; i++) {
        fprintf(outFile, "%d", array[i]);
        if (i < numElements - 1) {
            fprintf(outFile, " ");
        }
    }
    fclose(outFile);

    // Записываем время работы в файл
    FILE* timeFile = fopen("time.txt", "w");
    if (!timeFile) {
        fprintf(stderr, "Не удалось открыть файл времени.\n");
        free(array);
        destroyTaskQueue();
        CloseHandle(completion_semaphore);
        return 1;
    }

    fprintf(timeFile, "%.0f", duration);
    fclose(timeFile);

    // Освобождаем ресурсы
    free(array);
    destroyTaskQueue();
    CloseHandle(completion_semaphore);

    return 0;
}