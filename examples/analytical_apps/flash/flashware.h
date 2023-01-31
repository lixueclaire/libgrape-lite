/** Copyright 2022 Alibaba Group Holding Limited.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

#ifndef EXAMPLES_ANALYTICAL_APPS_FLASH_APP_FLASHWARE_H_
#define EXAMPLES_ANALYTICAL_APPS_FLASH_APP_FLASHWARE_H_

#include "grape/config.h"
#include "grape/communication/communicator.h"
#include "grape/parallel/parallel_engine.h"
#include "grape/parallel/parallel_message_manager.h"
#include "grape/worker/comm_spec.h"

#include "flash/flash_app_base.h"

namespace grape {
namespace flash {

/**
 * @brief The middle-ware of Flash.
 *
 * @tparam FRAG_T
 * @tparam VALUE_T
 */
template <typename FRAG_T, class VALUE_T>
class FlashWare : public Communicator, public ParallelEngine {
 public:
  using fragment_t = FRAG_T;
  using vid_t = typename fragment_t::vid_t;
  using value_t = VALUE_T;
  static const int SEG = 32;

  FlashWare() = default;
  ~FlashWare() {
    delete[] states_;
    delete[] next_states_;
    masters_.clear();
    mirrors_.clear();
  }

 public:
  void InitFlashWare(const CommSpec& comm_spec, const vid_t& n_vertices,
                     const bool& sync_all, const fragment_t& graph);
  void Start();
  void Terminate();

  void GetActiveVertices(std::vector<vid_t>& result);
  void GetActiveVerticesAndSetStates(std::vector<vid_t>& result);
  void GetActiveVertices(std::vector<vid_t>& result, Bitset& d);
  void SyncBitset(Bitset& tmp, Bitset& d);
  void SyncBitset(Bitset& b);

  inline value_t* Get(const vid_t& key);
  void PutNext(const vid_t& key, const value_t& value, const bool& flag = false,
               const int& tid = 0);
  void Barrier(bool flag = false);

 public:
  inline void SetInitFunc(const std::function<void(value_t&)>& f_init) {
    f_init_ = f_init;
  }
  inline void ReSetInitFunc() { f_init_ = nullptr; }
  inline void SetAggFunc(
      const std::function<void(const vid_t, const vid_t, const value_t&,
                               value_t&)>& f_agg) {
    f_agg_ = f_agg;
  }
  inline void ResetAggFunc() { f_agg_ = nullptr; }

 public:
  inline int GetPid() { return pid_; }
  inline vid_t GetSize() { return n_; }
  inline std::vector<vid_t>* GetMasters() { return &masters_; }
  inline std::vector<vid_t>* GetMirrors() { return &mirrors_; }
  inline int GetMasterPid(const vid_t& key) { return key % n_procs_; }
  inline bool IsMaster(const vid_t& key) { return GetMasterPid(key) == pid_; }
  inline bool IsDirty(const vid_t& key) { return is_dirty_.get_bit(key); }
  inline void SetDirty(const vid_t& key) { is_dirty_.set_bit(key); }
  inline void ResetDirty(const vid_t& key) { is_dirty_.reset_bit(key); }
  inline bool IsActive(const vid_t& key) { return is_active_.get_bit(key); }
  inline void SetActive(const vid_t& key) { is_active_.set_bit(key); }
  inline void ResetActive(const vid_t& key) { is_active_.reset_bit(key); }
  inline void SetStates(const vid_t& key) { states_[key] = next_states_[key]; }

 private:
  inline void ToSend(const int& pid, const vid_t& key, const int& tid);
  inline void Synchronize(const int& tid, const vid_t& key);
  inline void Synchronize(const vid_t& key);
  inline void SynchronizeAll();
  inline void Update(const int& tid, const vid_t& key);
  inline void UpdateAll();
  inline void ProcessMessage(const int& tid, const vid_t& key,
                             const value_t& value);
  inline void ProcessAllMessages(const bool& is_parallel);

 private:
  vid_t n_;
  vid_t n_loc_;
  int pid_;
  int n_procs_;
  int n_threads_;
  std::vector<vid_t> masters_;
  std::vector<vid_t> mirrors_;
  CommSpec comm_spec_;
  ParallelMessageManager messages_;
  bool sync_all_;
  Bitset nb_ids_;

  value_t* states_;
  value_t* next_states_;
  Bitset is_dirty_;
  Bitset is_active_;
  int step_;
  std::mutex* seg_mutex_;

  std::function<void(value_t&)> f_init_;
  std::function<void(const vid_t, const vid_t, const value_t&, value_t&)>
      f_agg_;
};

template <typename fragment_t, class value_t>
void FlashWare<fragment_t, value_t>::InitFlashWare(const CommSpec& comm_spec,
                                                   const vid_t& n_vertices,
                                                   const bool& sync_all,
                                                   const fragment_t& graph) {
  comm_spec_ = comm_spec;
  MPI_Barrier(comm_spec_.comm());
  InitParallelEngine();
  InitCommunicator(comm_spec_.comm());
  messages_.Init(comm_spec_.comm());
  messages_.InitChannels(thread_num());

  n_procs_ = comm_spec_.fnum();
  pid_ = comm_spec_.fid();
  n_threads_ = thread_num();
  seg_mutex_ = new std::mutex[SEG];

  n_ = n_vertices;
  n_loc_ = n_ / n_procs_ + (pid_ < (n_ % n_procs_) ? 1 : 0);
  states_ = new value_t[n_];
  next_states_ = new value_t[n_];
  is_dirty_.init(n_);
  is_active_.init(n_);

  masters_.clear();
  mirrors_.clear();
  for (vid_t i = 0; i < n_; i++)
    if (IsMaster(i))
      masters_.push_back(i);
    else
      mirrors_.push_back(i);

  sync_all_ = sync_all;
  nb_ids_.init(n_loc_ * n_procs_);
  if (!sync_all_) {
    ForEach(graph.InnerVertices(),
            [this, &graph](int tid, typename fragment_t::vertex_t v) {
              auto dsts = graph.IOEDests(v);
              vid_t lid = graph.GetId(v) / n_procs_;
              const fid_t* ptr = dsts.begin;
              while (ptr != dsts.end) {
                fid_t fid = *(ptr++);
                nb_ids_.set_bit(lid * n_procs_ + fid);
              }
            });
  }

  f_init_ = nullptr;
  f_agg_ = nullptr;
  step_ = 0;

  std::cout << "init flashware: " << n_procs_ << ' ' << pid_ << ' '
            << n_threads_ << ':' << n_loc_ << std::endl;
}

template <typename fragment_t, class value_t>
void FlashWare<fragment_t, value_t>::Start() {
  MPI_Barrier(comm_spec_.comm());
  messages_.Start();
  messages_.StartARound();
  step_++;
}

template <typename fragment_t, class value_t>
void FlashWare<fragment_t, value_t>::Terminate() {
  messages_.FinishARound();
  MPI_Barrier(comm_spec_.comm());
  messages_.Finalize();
  std::cout << "flashware terminate" << std::endl;
}

template <typename fragment_t, class value_t>
void FlashWare<fragment_t, value_t>::GetActiveVertices(
    std::vector<vid_t>& result) {
  result.clear();
  for (auto& u : masters_) {
    if (IsActive(u)) {
      result.push_back(u);
      ResetActive(u);
    }
  }
}

template <typename fragment_t, class value_t>
void FlashWare<fragment_t, value_t>::GetActiveVerticesAndSetStates(
    std::vector<vid_t>& result) {
  result.clear();
  for (auto& u : masters_) {
    if (IsActive(u)) {
      SetStates(u);
      result.push_back(u);
      ResetActive(u);
    }
  }
}

template <typename fragment_t, class value_t>
void FlashWare<fragment_t, value_t>::GetActiveVertices(
    std::vector<vid_t>& result, Bitset& d) {
  SyncBitset(is_active_, d);
  GetActiveVerticesAndSetStates(result);
}

template <typename fragment_t, class value_t>
void FlashWare<fragment_t, value_t>::SyncBitset(Bitset& tmp, Bitset& res) {
  if (res.get_size() < tmp.get_size())
    res.init(tmp.get_size());
  MPI_Allreduce(tmp.get_data(), res.get_data(), res.get_size_in_words(),
                MPI_UINT64_T, MPI_BOR, comm_spec_.comm());
}

template <typename fragment_t, class value_t>
void FlashWare<fragment_t, value_t>::SyncBitset(Bitset& b) {
  MPI_Allreduce(MPI_IN_PLACE, b.get_data(), b.get_size_in_words(),
                MPI_UINT64_T, MPI_BOR, comm_spec_.comm());
}

template <typename fragment_t, class value_t>
inline value_t* FlashWare<fragment_t, value_t>::Get(const vid_t& key) {
  return &states_[key];
}

template <typename fragment_t, class value_t>
void FlashWare<fragment_t, value_t>::PutNext(const vid_t& key,
                                             const value_t& value,
                                             const bool& flag, const int& tid) {
  if (!flag) {
    SetDirty(key);
    next_states_[key] = value;
    Synchronize(tid, key);
  } else {
    // seg_mutex_[key % SEG].lock();
    if (!IsDirty(key)) {
      SetDirty(key);
      if (f_init_ != nullptr)
        f_init_(next_states_[key]);
      else
        next_states_[key] = states_[key];
    }
    if (f_agg_ != nullptr)
      f_agg_(key, key, value, next_states_[key]);
    else
      next_states_[key] = value;
    // seg_mutex_[key % SEG].unlock();
  }
}

template <typename fragment_t, class value_t>
void FlashWare<fragment_t, value_t>::Barrier(bool flag) {
  if (flag) {
    UpdateAll();
    SynchronizeAll();
  }
  messages_.FinishARound();
  MPI_Barrier(comm_spec_.comm());
  messages_.StartARound();

  ProcessAllMessages(true);
  step_++;
}

template <typename fragment_t, class value_t>
inline void FlashWare<fragment_t, value_t>::ToSend(const int& pid,
                                                   const vid_t& key,
                                                   const int& tid) {
  messages_.SendVertexToFragment<vid_t, value_t>(pid, key, next_states_[key],
                                                 tid);
}

template <typename fragment_t, class value_t>
inline void FlashWare<fragment_t, value_t>::Synchronize(const int& tid,
                                                        const vid_t& key) {
  for (int i = 0; i < n_procs_; i++)
    if (i != pid_ &&
        (sync_all_ || (nb_ids_.get_bit(key / n_procs_ * n_procs_ + i))))
      ToSend(i, key, tid);
  ResetDirty(key);
  SetActive(key);
}

template <typename fragment_t, class value_t>
inline void FlashWare<fragment_t, value_t>::Synchronize(const vid_t& key) {
  for (int i = 0; i < n_procs_; i++)
    if (i != pid_ &&
        (sync_all_ || (nb_ids_.get_bit(key / n_procs_ * n_procs_ + i))))
      messages_.SendVertexToFragment<vid_t, value_t>(i, key, states_[key], 0);
}

template <typename fragment_t, class value_t>
inline void FlashWare<fragment_t, value_t>::SynchronizeAll() {
  ForEach(masters_.begin(), masters_.end(), [this](int tid, vid_t key) {
    if (IsDirty(key))
      this->Synchronize(tid, key);
  });
}

template <typename fragment_t, class value_t>
inline void FlashWare<fragment_t, value_t>::Update(const int& tid,
                                                   const vid_t& key) {
  ToSend(GetMasterPid(key), key, tid);
  ResetDirty(key);
}

template <typename fragment_t, class value_t>
inline void FlashWare<fragment_t, value_t>::UpdateAll() {
  ForEach(mirrors_.begin(), mirrors_.end(), [this](int tid, vid_t key) {
    if (IsDirty(key))
      this->Update(tid, key);
  });

  messages_.FinishARound();
  MPI_Barrier(comm_spec_.comm());
  messages_.StartARound();
  ProcessAllMessages(f_agg_ == nullptr);
}

template <typename fragment_t, class value_t>
inline void FlashWare<fragment_t, value_t>::ProcessMessage(
    const int& tid, const vid_t& key, const value_t& value) {
  if (IsMaster(key)) {
    SetDirty(key);
    if (f_agg_ == nullptr)
      next_states_[key] = value;
    else
      f_agg_(key, key, value, next_states_[key]);
  } else {
    states_[key] = value;
  }
}

template <typename fragment_t, class value_t>
inline void FlashWare<fragment_t, value_t>::ProcessAllMessages(
    const bool& is_parallel) {
  int n_threads = is_parallel ? n_threads_ : 1;
  messages_.ParallelProcess<std::pair<vid_t, value_t>>(
      n_threads, [this](int tid, const std::pair<vid_t, value_t>& msg) {
        this->ProcessMessage(tid, msg.first, msg.second);
      });
}

}  // namespace flash
}  // namespace grape

#endif  // EXAMPLES_ANALYTICAL_APPS_FLASH_APP_FLASHWARE_H_
