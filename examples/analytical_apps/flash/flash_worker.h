/** Copyright 2020 Alibaba Group Holding Limited.

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

#ifndef GRAPE_WORKER_FLASH_WORKER_H_
#define GRAPE_WORKER_FLASH_WORKER_H_

#include <mpi.h>

#include <memory>
#include <ostream>
#include <type_traits>
#include <utility>

#include "flash/flash_app_base.h"
#include "flash/flashware.h"
#include "flash/vertex_subset.h"

namespace grape {
namespace flash {

/**
 * @brief A worker manages the computation flow of Flash.
 *
 * @tparam APP_T
 */
template <typename APP_T>
class FlashWorker {
  static_assert(std::is_base_of<FlashAppBase<typename APP_T::fragment_t,
                                             typename APP_T::value_t>,
                                APP_T>::value,
                "FlashWorker should work with App");

 public:
  using fragment_t = typename APP_T::fragment_t;
  using value_t = typename APP_T::value_t;
  using vertex_t = typename APP_T::vertex_t;
  using vid_t = typename APP_T::vid_t;

  FlashWorker(std::shared_ptr<APP_T> app, std::shared_ptr<fragment_t> graph)
      : app_(app), graph_(graph) {
    prepare_conf_.message_strategy = APP_T::message_strategy;
    prepare_conf_.need_split_edges = APP_T::need_split_edges;
    prepare_conf_.need_split_edges_by_fragment =
        APP_T::need_split_edges_by_fragment;
  }
  ~FlashWorker() = default;

  void Init(const CommSpec& comm_spec,
            const ParallelEngineSpec& pe_spec = DefaultParallelEngineSpec()) {
    graph_->PrepareToRunApp(comm_spec_, prepare_conf_);
    comm_spec_ = comm_spec;
    MPI_Barrier(comm_spec_.comm());

    fw_ = new grape::flash::FlashWare<fragment_t, value_t>;
    fw_->InitFlashWare(comm_spec_, graph_->GetTotalVerticesNum() + 1,
                       app_->sync_all_, *graph_);
    All.fw = fw_;
    std::vector<vid_t>* masters = fw_->GetMasters();
    for (auto it = masters->begin(); it != masters->end(); it++) {
      All.AddV(*it);
    }
  }

  void Finalize() {}

  template <class... Args>
  void Query(Args&&... args) {
    fw_->Start();
    app_->Run(*graph_, std::forward<Args>(args)...);
    fw_->Terminate();
  }

  const TerminateInfo& GetTerminateInfo() const {
    return fw_->messages_.GetTerminateInfo();
  }

  void Output(std::ostream& os) {
    auto inner_vertices = graph_->InnerVertices();
    for (auto v : inner_vertices) {
      vid_t id = graph_->GetId(v);
      auto p = app_->Res(GetV(id));
      if (p != nullptr)
        os << id << " " << *p << std::endl;
    }
  }

 private:
  std::shared_ptr<APP_T> app_;
  std::shared_ptr<fragment_t> graph_;
  grape::flash::FlashWare<fragment_t, value_t>* fw_;
  //std::shared_ptr<grape::flash::FlashWare<fragment_t, value_t> > fw_;
  CommSpec comm_spec_;
  PrepareConf prepare_conf_;
};

}  // namespace flash
}  // namespace grape

#endif  // GRAPE_WORKER_FLASH_WORKER_H_
