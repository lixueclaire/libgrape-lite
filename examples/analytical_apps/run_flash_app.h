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

#ifndef EXAMPLES_ANALYTICAL_APPS_RUN_FLASH_APP_H_
#define EXAMPLES_ANALYTICAL_APPS_RUN_FLASH_APP_H_

#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <grape/fragment/immutable_edgecut_fragment.h>
#include <grape/fragment/loader.h>
#include <grape/grape.h>
#include <grape/util.h>
#include <grape/vertex_map/global_vertex_map.h>

#include <algorithm>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#ifdef GRANULA
#include "thirdparty/atlarge-research-granula/granula.hpp"
#endif

#include "timer.h"
#include "flags.h"
#include "flash/flash_worker.h"
#include "flash/bfs/bfs.h"
#include "flash/cc/cc-opt.h"
#include "flash/cc/cc.h"
#include "flash/pagerank/pagerank.h"

#ifndef __AFFINITY__
#define __AFFINITY__ false
#endif

namespace grape {
namespace flash {

void Init() {
  if (FLAGS_deserialize && FLAGS_serialization_prefix.empty()) {
    LOG(FATAL) << "Please assign a serialization prefix.";
  } else if (FLAGS_efile.empty()) {
    LOG(FATAL) << "Please assign input edge files.";
  } else if (FLAGS_segmented_partition) {
    LOG(FATAL) << "FLASH dosen't support Segmented Partitioner. ";
  }

  if (access(FLAGS_out_prefix.c_str(), 0) != 0) {
    mkdir(FLAGS_out_prefix.c_str(), 0777);
  }

  grape::InitMPIComm();
  CommSpec comm_spec;
  comm_spec.Init(MPI_COMM_WORLD);
  if (comm_spec.worker_id() == kCoordinatorRank) {
    VLOG(1) << "Workers of libgrape-lite initialized.";
  }
}

void Finalize() {
  grape::FinalizeMPIComm();
  VLOG(1) << "Workers finalized.";
}

template <typename FRAG_T, typename APP_T, typename... Args>
void DoQuery(std::shared_ptr<FRAG_T> fragment, std::shared_ptr<APP_T> app,
             const CommSpec& comm_spec, const ParallelEngineSpec& spec,
             const std::string& out_prefix, Args... args) {
  timer_next("load application");
  using worker_t = grape::flash::FlashWorker<APP_T>;
  auto worker = std::make_shared<worker_t>(app, fragment);
  worker->Init(comm_spec, spec);

  timer_next("run algorithm");
  worker->Query(std::forward<Args>(args)...);

  timer_next("print output");
  std::ofstream ostream;
  std::string output_path =
      grape::GetResultFilename(out_prefix, fragment->fid());
  ostream.open(output_path);
  worker->Output(ostream);
  ostream.close();
  worker->Finalize();
  timer_end();
  VLOG(1) << "Worker-" << comm_spec.worker_id() << " finished: " << output_path;
}

template <typename FRAG_T, typename APP_T, typename... Args>
void CreateAndQuery(const CommSpec& comm_spec, const std::string& out_prefix,
                    int fnum, const ParallelEngineSpec& spec, Args... args) {
  timer_next("load graph");
  LoadGraphSpec graph_spec = DefaultLoadGraphSpec();
  graph_spec.set_directed(FLAGS_directed);
  graph_spec.set_rebalance(false, 0);
  if (FLAGS_deserialize) {
    graph_spec.set_deserialize(true, FLAGS_serialization_prefix);
  } else if (FLAGS_serialize) {
    graph_spec.set_serialize(true, FLAGS_serialization_prefix);
  }

  std::shared_ptr<FRAG_T> fragment =
      LoadGraph<FRAG_T>(FLAGS_efile, FLAGS_vfile, comm_spec, graph_spec);
  auto app = std::make_shared<APP_T>();
  DoQuery<FRAG_T, APP_T, Args...>(fragment, app, comm_spec, spec, out_prefix,
                                  args...);                              
}

struct BFS_TYPE {
  int dis; 
};

struct CC_TYPE {
  int tag;
};

struct CC_OPT_TYPE {
  long long cid;
};

struct PR_TYPE {
  int deg;
  float val, next;
};
inline InArchive& operator<<(InArchive& in_archive, const PR_TYPE& v) {
  in_archive << v.deg << v.val;
  return in_archive;
}
inline OutArchive& operator>>(OutArchive& out_archive, PR_TYPE& v) {
  out_archive >> v.deg >> v.val;
  return out_archive;
}

void RunFlash() {
  CommSpec comm_spec;
  comm_spec.Init(MPI_COMM_WORLD);

  bool is_coordinator = comm_spec.worker_id() == grape::kCoordinatorRank;
  timer_start(is_coordinator);

  std::string name = FLAGS_application;
  std::string efile = FLAGS_efile;
  std::string vfile = FLAGS_vfile;
  std::string out_prefix = FLAGS_out_prefix;

  auto spec = MultiProcessSpec(comm_spec, __AFFINITY__);
  if (FLAGS_app_concurrency != -1) {
    spec.thread_num = FLAGS_app_concurrency;
    if (__AFFINITY__) {
      if (spec.cpu_list.size() >= spec.thread_num) {
        spec.cpu_list.resize(spec.thread_num);
      } else {
        uint32_t num_to_append = spec.thread_num - spec.cpu_list.size();
        for (uint32_t i = 0; i < num_to_append; ++i) {
          spec.cpu_list.push_back(spec.cpu_list[i]);
        }
      }
    }
  }
  int fnum = comm_spec.fnum();

  if (name == "bfs") {
    using GraphType =
        grape::ImmutableEdgecutFragment<int32_t, uint32_t, grape::EmptyType,
                                        grape::EmptyType,
                                        LoadStrategy::kBothOutIn>;
    using AppType = grape::flash::BFSFlash<GraphType, BFS_TYPE>;
    CreateAndQuery<GraphType, AppType>(comm_spec, out_prefix, fnum, spec,
                                       FLAGS_bfs_source);
  } else if (name == "pagerank") {
    using GraphType =
        grape::ImmutableEdgecutFragment<int32_t, uint32_t, grape::EmptyType,
                                        grape::EmptyType,
                                        LoadStrategy::kBothOutIn>;
    using AppType = grape::flash::PRFlash<GraphType, PR_TYPE>;
    CreateAndQuery<GraphType, AppType>(comm_spec, out_prefix, fnum, spec,
                                       FLAGS_pr_mr, FLAGS_pr_d);
  } else if (name == "cc") {
    using GraphType =
        grape::ImmutableEdgecutFragment<int32_t, uint32_t, grape::EmptyType,
                                        grape::EmptyType,
                                        LoadStrategy::kBothOutIn>;
    using AppType = grape::flash::CCFlash<GraphType, CC_TYPE>;
    CreateAndQuery<GraphType, AppType>(comm_spec, out_prefix, fnum, spec);
  } else if (name == "cc-opt") {
    using GraphType =
        grape::ImmutableEdgecutFragment<int32_t, uint32_t, grape::EmptyType,
                                        grape::EmptyType,
                                        LoadStrategy::kBothOutIn>;
    using AppType = grape::flash::CCOptFlash<GraphType, CC_OPT_TYPE>;
    CreateAndQuery<GraphType, AppType>(comm_spec, out_prefix, fnum, spec);
  } else {
    LOG(FATAL) << "Invalid app name: " << name;
  }
}

}  // namespace flash
}  // namespace grape
#endif  // EXAMPLES_ANALYTICAL_APPS_RUN_FLASH_APP_H_
