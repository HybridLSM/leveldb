#ifndef STORAGE_LEVELDB_DB_SSTSCORETABLE_H_
#define STORAGE_LEVELDB_DB_SSTSCORETABLE_H_

#include <unordered_map>
#include <cassert>

namespace leveldb {
struct ScoreSst {
  int score;
  uint64_t sst_id;
};

class ScoreTable {
  public:
    explicit ScoreTable();
    ~ScoreTable(){}

    void AddScore(uint64_t sst_id);
    void RemoveSstScore(uint64_t sst_id);
    void ResetHighest();
    bool Find(uint64_t sst_id);
    ScoreSst GetHighScoreSst();

  private:
    std::unordered_map<uint64_t, int> score_table_;
    ScoreSst cur_highest_;
};

ScoreTable::ScoreTable() : score_table_(), cur_highest_() {
    cur_highest_.score = 0;
    cur_highest_.sst_id = 0;
}

void ScoreTable::AddScore(uint64_t sst_id) {
  auto it = score_table_.find(sst_id);
  if (it != score_table_.end()) {
    score_table_[sst_id] = it->second + 1;
  } else {
    score_table_[sst_id] = 1;
  }
  if (score_table_[sst_id] > cur_highest_.score) {
    cur_highest_.sst_id = sst_id;
    cur_highest_.score = score_table_[sst_id];
  }
}

void ScoreTable::RemoveSstScore(uint64_t sst_id) {
  score_table_.erase(sst_id);
}

void ScoreTable::ResetHighest() {
  cur_highest_.score = 0;
  for (const auto& it : score_table_) {
    if (it.second > cur_highest_.score) {
      cur_highest_.score = it.second;
      cur_highest_.sst_id = it.first;
    }
  }
}

bool ScoreTable::Find(uint64_t sst_id) {
  return score_table_.find(sst_id) != score_table_.end();
}

ScoreSst ScoreTable::GetHighScoreSst() {
  return cur_highest_;
}

}
#endif