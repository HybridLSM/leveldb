#ifndef STORAGE_LEVELDB_DB_DIRECTORYMANAGE_H_
#define STORAGE_LEVELDB_DB_DIRECTORYMANAGE_H_

#include <cassert>
#include <cstdint>
#include <string>
#include <queue>
#include <unordered_set>
#include <unordered_map>

#include "db/filename.h"
#include "db/dbformat.h"
#include "db/version_edit.h"
#include "leveldb/status.h"
#include "leveldb/env.h"
#include "leveldb/table.h"
#include "port/port_stdcxx.h"
#include "port/thread_annotations.h"
#include "util/mutexlock.h"

namespace leveldb {

class DirectoryManager {
  // class DirectoryManagerBackgroundThread;

 public:
  DirectoryManager(std::string ssd_path, std::string hdd_path, Env* const env)
      : ssd_path_(ssd_path),
        hdd_path_(hdd_path),
        env_(env),
        produce_s2h_(&mutex_),
        consume_s2h_(&mutex_),
        consume_h2s_(&mutex_),
        s2h_finished_(&mutex_),
        h2s_finished_(&mutex_),
        end_(false) {
          // thread 1 SSD -> HDD
          env_->StartThread(&DirectoryManager::BGWorkSSD2HDD, this);

          // thread 2 HDD -> SSD
          env_->StartThread(&DirectoryManager::BGWorkHDD2SSD, this);
        }

  ~DirectoryManager() {
    mutex_.Lock();
    end_ = true;

    // shutdown thread 1
    consume_s2h_.SignalAll();
    s2h_finished_.Wait();

    // shutdown thread 2
    consume_h2s_.SignalAll();
    h2s_finished_.Wait();
    mutex_.Unlock();
  }

  Disk DetermineFileDisk(int level, FileArea area=FileArea::fNormal) {
    assert(area != FileArea::fUnKnown);
    
    Disk type;
    if (level == 0 || area == FileArea::fHot || area == FileArea::fWarm)
      type = Disk::SSD;
    else
      type = Disk::HDD;

    return type;
  }
  
  // Compaction in HDD
  std::string CompactionDefaultFilePath() const {
    return hdd_path_;
  }

  std::string GetFileDiskPath(int level, FileArea area) {
    assert(area != FileArea::fUnKnown);
    
    Disk type = DetermineFileDisk(level, area);
    if (type == Disk::SSD)
      return ssd_path_;
    else
      return hdd_path_;
  }

  std::string GetFileDiskPath(Disk type) const {
    if (type == Disk::SSD)
      return ssd_path_;
    else
      return hdd_path_;
  }

  // need revision
  std::string GetFileDiskPath(uint64_t file_num, Status* status) {
    MutexLock l(&mutex_);
    bool is_ssd = ssd_file_set_.find(file_num) != ssd_file_set_.end();
    bool is_hdd = hdd_file_set_.find(file_num) != hdd_file_set_.end();
    bool exists_error = failed_files_status_.find(file_num) != failed_files_status_.end();

    assert(is_ssd || is_hdd || exists_error);
    // may have file not found error
    if (exists_error) {
      *status = failed_files_status_[file_num];
      return "";
    } 

    *status = Status::OK();
    if (is_ssd)
      return ssd_path_;
    else
      return hdd_path_;

  }

  void RecordFileDisk(uint64_t file_num, Disk type) {
    MutexLock l(&mutex_);
    if (type == Disk::SSD) {
      ssd_file_set_.insert(file_num);
    }
    else {
      hdd_file_set_.insert(file_num);
    }
  }

  void RecordFileDisk(uint64_t file_num, std::string file_path) {
    MutexLock l(&mutex_);
    if (file_path == ssd_path_) {
      ssd_file_set_.insert(file_num);
    }
    else if (file_path == hdd_path_) {
      hdd_file_set_.insert(file_num);
    }

  }
  
  void EraseFileDisk(uint64_t file_num, std::string file_path) {
    MutexLock l(&mutex_);
    if (file_path == ssd_path_) {
      ssd_file_set_.erase(file_num);
    }
    else if (file_path == hdd_path_) {
      hdd_file_set_.erase(file_num);
    }
  }

  void EraseFileDisk(uint64_t file_num, Disk type) {
    MutexLock l(&mutex_);
    if (type == Disk::SSD) {
      ssd_file_set_.erase(file_num);
    }
    else if (type == Disk::HDD) {;
      hdd_file_set_.erase(file_num);
    }
  }

  // Trivial Move
  // Only direction from ssd to hdd
  Status FileTrivialMove(int level, uint64_t file_num) {
    MutexLock l(&mutex_);
    bool in_hdd = hdd_file_set_.find(file_num) != hdd_file_set_.end();
    
    mutex_.Unlock();
    if (level == 0 && !in_hdd) {
      std::string src = TableFileName(ssd_path_, file_num);
      std::string tgt = TableFileName(hdd_path_, file_num);
      Status status = env_->MigrationFile(src, tgt);
      if (status.ok()) {
        mutex_.Lock();
        hdd_file_set_.insert(file_num);
      }
      return status;
    }

    // Indeed Trivial Move 
    return Status::OK();
  }
  
  // File migration from SSD to HDD
  void FileMigrationSSD2HDD(std::vector<FileMetaData*> hw_input) {
    MutexLock l(&mutex_);
    for (auto it : hw_input)
      files_to_migrate_s2h_.push(it->number);
    consume_s2h_.Signal();
  }

  // File migration from HDD to SSD
  void FileMigrationHDD2SSD(uint64_t file_num) {
    MutexLock l(&mutex_);
    files_to_migrate_h2s_.push(file_num);
    consume_h2s_.Signal();
  }

  std::string CompactionGetFileDiskPath(uint64_t file_num, Status* status) {
    MutexLock l(&mutex_);
    bool in_hdd = hdd_file_set_.find(file_num) != hdd_file_set_.end();
    bool migration_failed = failed_files_status_.find(file_num) != failed_files_status_.end();
    while (!in_hdd && !migration_failed) {
      produce_s2h_.Wait(); // wait for migration
      in_hdd = hdd_file_set_.find(file_num) != hdd_file_set_.end();
      migration_failed = failed_files_status_.find(file_num) != failed_files_status_.end();
    }

    assert(in_hdd);
    if (migration_failed)
      *status = failed_files_status_[file_num];
    else
      *status = Status::OK();
    return hdd_path_;
  }

  // void RecordDiskFile(uint64_t file_num, Disk type) {
  //   if (type == Disk::SSD)
  //     ssd_file_set_.insert(file_num);
  //   else if (type == Disk::HDD)
  //     hdd_file_set_.insert(file_num);
  // }
 private:
  std::string ssd_path_;
  std::string hdd_path_;
  std::unordered_set<uint64_t> ssd_file_set_ GUARDED_BY(mutex_); 
  std::unordered_set<uint64_t> hdd_file_set_ GUARDED_BY(mutex_);
  
  Env* const env_;
  
  port::Mutex mutex_;
  port::CondVar produce_s2h_;
  port::CondVar consume_s2h_;
  port::CondVar consume_h2s_;
  port::CondVar s2h_finished_;
  port::CondVar h2s_finished_;
  std::queue<uint64_t> files_to_migrate_s2h_ GUARDED_BY(mutex_);
  std::queue<uint64_t> files_to_migrate_h2s_ GUARDED_BY(mutex_);
  std::unordered_map<uint64_t, Status> failed_files_status_ GUARDED_BY(mutex_);

  bool end_;
  static
  void BGWorkSSD2HDD(void *master) {
    reinterpret_cast<DirectoryManager*> (master)->AsyncFileMigrationSSD2HDD_();
  }

  void AsyncFileMigrationSSD2HDD_() {
    MutexLock l(&mutex_);
    while (!end_) {

      // Wait until new work come in
      while (files_to_migrate_s2h_.empty()) {
        consume_s2h_.Wait();
        assert(mutex_.AssertHeld());
        
        if (end_) {
          break;
        }
      }

      // Get one file to migrate
      if (!files_to_migrate_s2h_.empty()) {
        bool in_ssd, in_hdd;
        bool has_file_to_migrate;
        uint64_t file_num;
        do {
          file_num = files_to_migrate_s2h_.front();
          files_to_migrate_s2h_.pop();

          in_ssd = ssd_file_set_.find(file_num) != ssd_file_set_.end();
          in_hdd = hdd_file_set_.find(file_num) != hdd_file_set_.end();
          has_file_to_migrate = in_ssd && !in_hdd;
        } while (!files_to_migrate_s2h_.empty() && !has_file_to_migrate);
      
        mutex_.Unlock();
        
        if (has_file_to_migrate) {
          // Migrate from SSD to HDD
          std::string src = TableFileName(ssd_path_, file_num);
          std::string tgt = TableFileName(hdd_path_, file_num);

          Status status = env_->MigrationFile(src, tgt);
          mutex_.Lock();
          if (status.ok()) {
            hdd_file_set_.insert(file_num);
          } else {
            failed_files_status_[file_num] = status;
          }
          mutex_.Unlock();
          produce_s2h_.Signal();
        }

        mutex_.Lock();
      }
    }
    s2h_finished_.SignalAll();
  }

  static
  void BGWorkHDD2SSD(void *master) {
    reinterpret_cast<DirectoryManager*> (master)->AsyncFileMigrationHDD2SSD_();
  }

  void AsyncFileMigrationHDD2SSD_() {
    MutexLock l(&mutex_);
    while (!end_) {
      // Wait until new work come in
      while (files_to_migrate_h2s_.empty()) {
        consume_h2s_.Wait();
        assert(mutex_.AssertHeld());
        
        if (end_) {
          break;
        }
      }

      // Get one file to migrate
      if (!files_to_migrate_h2s_.empty()) {
        bool in_ssd, in_hdd;
        bool has_file_to_migrate;
        uint64_t file_num;
        do {
          file_num = files_to_migrate_h2s_.front();
          files_to_migrate_h2s_.pop();

          in_ssd = ssd_file_set_.find(file_num) != ssd_file_set_.end();
          in_hdd = hdd_file_set_.find(file_num) != hdd_file_set_.end();
          has_file_to_migrate = !in_ssd && in_hdd;
        } while (!files_to_migrate_h2s_.empty() && !has_file_to_migrate);
      
        mutex_.Unlock();
        
        if (has_file_to_migrate) {
          // Migrate from HDD to SSD
          std::string src = TableFileName(hdd_path_, file_num);
          std::string tgt = TableFileName(ssd_path_, file_num);

          Status status = env_->MigrationFile(src, tgt);
          mutex_.Lock();
          if (status.ok()) {
            ssd_file_set_.insert(file_num);
          } else {
            failed_files_status_[file_num] = status;
          }
          mutex_.Unlock();
        }

        mutex_.Lock();
      }
    }
    h2s_finished_.SignalAll();
  }

//   // Use example from env_posix.cc
//   class DirectoryManagerBackgroundThread {
//    public:
//     DirectoryManagerBackgroundThread(Env* const env)
//         : env_(env),
//           background_work_cv_(&background_work_mutex_),
//           started_background_thread_(false) {}

//     void Schedule(
//         void (*background_work_function)(void* background_work_arg),
//         void* background_work_arg) {
//       background_work_mutex_.Lock();

//       // Start the background thread, if we haven't done so already.
//       if (!started_background_thread_) {
//         started_background_thread_ = true;
//         env_->StartThread(BackgroundThreadEntryPoint, this);
//       }

//       if (background_work_queue_.empty()) {
//         background_work_cv_.Signal();
//       }

//       background_work_queue_.emplace(background_work_function, background_work_arg);
//       background_work_mutex_.Unlock();
//     }

//    private:
//     Env* const env_;
//     port::Mutex background_work_mutex_;
//     port::CondVar background_work_cv_ GUARDED_BY(background_work_mutex_);
//     bool started_background_thread_ GUARDED_BY(background_work_mutex_);

//     struct BackgroundWorkItem {
//       explicit BackgroundWorkItem(void (*function)(void* arg), void* arg)
//           : function(function), arg(arg) {}

//       void (*const function)(void*);
//       void* const arg;
//     };
//     std::queue<BackgroundWorkItem> background_work_queue_
//         GUARDED_BY(background_work_mutex_);

//     static void BackgroundThreadEntryPoint(void* p) {
//       DirectoryManagerBackgroundThread* ptr = reinterpret_cast<DirectoryManagerBackgroundThread*>(p);
//       ptr->BackgroundThreadMain();
//     }

//     void BackgroundThreadMain() {
//       while (true) {
//         background_work_mutex_.Lock();

//         // Wait until there is work to be done.
//         while (background_work_queue_.empty()) {
//           background_work_cv_.Wait();
//         }

//         assert(!background_work_queue_.empty());
//         auto background_work_function = background_work_queue_.front().function;
//         void* background_work_arg = background_work_queue_.front().arg;
//         background_work_queue_.pop();

//         background_work_mutex_.Unlock();
//         background_work_function(background_work_arg);
//       }
//     }
//   };
  };

}

#endif