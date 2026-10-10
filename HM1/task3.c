#include <pthread.h>
#include <stdlib.h>
#include <stdint.h>
#include <errno.h>

typedef struct {
    pthread_mutex_t mutex;
    pthread_cond_t cv_read;
    pthread_cond_t cv_write;
    uint64_t count_readers;
    uint64_t count_waiting_readers;
    uint64_t count_waiting_writers;
    uint8_t is_writer_active; // 0 - NO, 1 - YES
    pthread_t rank_writer;
} my_rwlock_t;

int32_t rwlock_init(my_rwlock_t* lock) {
    if (lock == NULL) return EINVAL;
    int rc;
    if ((rc = pthread_mutex_init(&lock->mutex, NULL)) != 0) return rc;
    if ((rc = pthread_cond_init(&lock->cv_read, NULL)) != 0) {
        pthread_mutex_destroy(&lock->mutex);
        return rc;
    }
    if ((rc = pthread_cond_init(&lock->cv_write, NULL)) != 0) {
        pthread_mutex_destroy(&lock->mutex);
        pthread_cond_destroy(&lock->cv_read);
        return rc;
    }
    lock->count_readers = 0;
    lock->count_waiting_readers = 0;
    lock->count_waiting_writers = 0;
    lock->is_writer_active = 0;
    return 0;
}

int32_t rwlock_destroy(my_rwlock_t* lock) {
    if (lock == NULL) return EINVAL;
    int rc;
    if ((rc = pthread_mutex_lock(&lock->mutex)) != 0) return rc;
    if (lock->count_readers != 0 || lock->is_writer_active ||
        lock->count_waiting_readers != 0 || lock->count_waiting_writers != 0) {
        pthread_mutex_unlock(&lock->mutex);
        return EBUSY;
    }
    pthread_mutex_unlock(&lock->mutex);
    if ((rc = pthread_cond_destroy(&lock->cv_read)) != 0) return rc;
    if ((rc = pthread_cond_destroy(&lock->cv_write)) != 0) return rc;
    return pthread_mutex_destroy(&lock->mutex);
}

int32_t rwlock_rdlock(my_rwlock_t* lock) {
    if (lock == NULL) return EINVAL;
    int rc;
    if ((rc = pthread_mutex_lock(&lock->mutex)) != 0) return rc;
    if (lock->is_writer_active || lock->count_waiting_writers > 0) {
        lock->count_waiting_readers++;
        while (lock->is_writer_active || lock->count_waiting_writers > 0) {
            if ((rc = pthread_cond_wait(&lock->cv_read, &lock->mutex)) != 0) {
                lock->count_waiting_readers--;
                pthread_mutex_unlock(&lock->mutex);
                return rc;
            }
        }
        lock->count_waiting_readers--;
    }
    lock->count_readers++;
    return pthread_mutex_unlock(&lock->mutex);
}

int32_t rwlock_wrlock(my_rwlock_t* lock) {
    if (lock == NULL) return EINVAL;
    int rc;
    if ((rc = pthread_mutex_lock(&lock->mutex)) != 0) return rc;
    if (lock->is_writer_active || lock->count_readers > 0) {
        lock->count_waiting_writers++;
        while (lock->is_writer_active || lock->count_readers > 0) {
            if ((rc = pthread_cond_wait(&lock->cv_write, &lock->mutex)) != 0) {
                lock->count_waiting_writers--;
                pthread_mutex_unlock(&lock->mutex);
                return rc;
            }
        }
        lock->count_waiting_writers--;
    }
    lock->is_writer_active = 1;
    lock->rank_writer = pthread_self();
    return pthread_mutex_unlock(&lock->mutex);
}

int32_t rwlock_unlock(my_rwlock_t* lock) {
    if (lock == NULL) return EINVAL;
    int rc;
    if ((rc = pthread_mutex_lock(&lock->mutex)) != 0) return rc;
    if (lock->is_writer_active) {
        if (!pthread_equal(lock->rank_writer, pthread_self())) {
            pthread_mutex_unlock(&lock->mutex);
            return EPERM;
        }
        lock->is_writer_active = 0;
    } else if (lock->count_readers > 0) {
        lock->count_readers--;
    } else {
        pthread_mutex_unlock(&lock->mutex);
        return EPERM;
    }
    if (lock->count_waiting_writers > 0 && 
        !lock->is_writer_active && lock->count_readers == 0) {
        if ((rc = pthread_cond_signal(&lock->cv_write)) != 0) {
            pthread_mutex_unlock(&lock->mutex);
            return rc;
        }
    }
    else if (lock->count_waiting_writers == 0 && lock->count_waiting_readers > 0) {
        if ((rc = pthread_cond_broadcast(&lock->cv_read)) != 0) {
            pthread_mutex_unlock(&lock->mutex);
            return rc;
        }
    }
    return pthread_mutex_unlock(&lock->mutex);
}
