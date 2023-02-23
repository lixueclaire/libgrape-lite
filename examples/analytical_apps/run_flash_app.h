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
#include "flash/flash_flags.h"
#include "flash/flash_worker.h"
#include "flash/traversal/bfs.h"
#include "flash/traversal/bfs-undirected.h"
#include "flash/traversal/dfs.h"
#include "flash/traversal/dfs-undirected.h"
#include "flash/traversal/sssp.h"
#include "flash/traversal/sssp-dlt-step.h"
#include "flash/traversal/random-walk.h"
#include "flash/centrality/bc.h"
#include "flash/centrality/bc-undirected.h"
#include "flash/centrality/katz.h"
#include "flash/centrality/katz-undirected.h"
#include "flash/centrality/eigenvec.h"
#include "flash/centrality/closeness.h"
#include "flash/centrality/harmonic.h"
#include "flash/connectivity/cc.h"
#include "flash/connectivity/cc-opt.h"
#include "flash/connectivity/cc-block.h"
#include "flash/connectivity/cc-union.h"
#include "flash/connectivity/cc-log.h"
#include "flash/connectivity/scc.h"
#include "flash/connectivity/scc-2.h"
#include "flash/connectivity/bcc.h"
#include "flash/connectivity/bcc-2.h"
#include "flash/connectivity/cut-point.h"
#include "flash/connectivity/cut-point-2.h"
#include "flash/connectivity/bridge.h"
#include "flash/connectivity/bridge-2.h"
#include "flash/matching/mis.h"
#include "flash/matching/mis-2.h"
#include "flash/matching/mm.h"
#include "flash/matching/mm-opt.h"
#include "flash/matching/mm-opt-2.h"
#include "flash/matching/min-edge-cover.h"
#include "flash/matching/min-cover.h"
#include "flash/matching/min-cover-greedy.h"
#include "flash/matching/min-cover-greedy-2.h"
#include "flash/matching/min-dominating-set.h"
#include "flash/matching/min-dominating-set-2.h"
#include "flash/ranking/pagerank.h"
#include "flash/ranking/articlerank.h"
#include "flash/ranking/ppr.h"
#include "flash/ranking/hits.h"
#include "flash/subgraph/triangle.h"
#include "flash/subgraph/3-path.h"
#include "flash/subgraph/tailed-triangle.h"
#include "flash/subgraph/rectangle.h"
#include "flash/subgraph/diamond.h"
#include "flash/subgraph/k-clique.h"
#include "flash/subgraph/k-clique-2.h"
#include "flash/subgraph/matrix-fac.h"
#include "flash/subgraph/densest-sub-2-approx.h"
#include "flash/measurement/msf.h"
#include "flash/measurement/msf-block.h"
#include "flash/measurement/k-center.h"
#include "flash/core/k-core-search.h"
#include "flash/core/core.h"
#include "flash/core/core-2.h"
#include "flash/core/ab-core.h"
#include "flash/core/onion-layer-ordering.h"
#include "flash/core/degeneracy-ordering.h"
#include "flash/clustering/color.h"
#include "flash/clustering/lpa.h"
#include "flash/clustering/lpa-by-color.h"
#include "flash/clustering/ego-net.h"
#include "flash/clustering/clustering-coeff.h"
#include "flash/clustering/fluid-community.h"
#include "flash/clustering/fluid-by-color.h"

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

struct EMPTY_TYPE { };

struct BFS_TYPE {
  int dis; 
};

struct RANDOM_WALK_TYPE {
  std::vector<int> from, from2, walk;
};
inline InArchive& operator<<(InArchive& in_archive, const RANDOM_WALK_TYPE& v) {
  in_archive << v.from << v.from2;
  return in_archive;
}
inline OutArchive& operator>>(OutArchive& out_archive, RANDOM_WALK_TYPE& v) {
  out_archive >> v.from >> v.from2;
  return out_archive;
}

struct DFS_TYPE {
  int f, deg, pre;
};

struct SSSP_TYPE {
  float dis; 
};

struct BC_TYPE {
  char d;
  float b, c;
};

struct KATZ_TYPE {
  float val, next;
};
inline InArchive& operator<<(InArchive& in_archive, const KATZ_TYPE& v) {
  in_archive << v.val;
  return in_archive;
}
inline OutArchive& operator>>(OutArchive& out_archive, KATZ_TYPE& v) {
  out_archive >> v.val;
  return out_archive;
}

struct CLOSENESS_TYPE {
  int64_t seen, cnt;
  double val;
};
inline InArchive& operator<<(InArchive& in_archive, const CLOSENESS_TYPE& v) {
  in_archive << v.seen;
  return in_archive;
}
inline OutArchive& operator>>(OutArchive& out_archive, CLOSENESS_TYPE& v) {
  out_archive >> v.seen;
  return out_archive;
}

struct HARMONIC_TYPE {
  int64_t seen;
  double val;
};
inline InArchive& operator<<(InArchive& in_archive, const HARMONIC_TYPE& v) {
  in_archive << v.seen;
  return in_archive;
}
inline OutArchive& operator>>(OutArchive& out_archive, HARMONIC_TYPE& v) {
  out_archive >> v.seen;
  return out_archive;
}

struct CC_TYPE {
  int tag;
};

struct CC_OPT_TYPE {
  int64_t cid;
};

struct CC_LOG_TYPE {
  int p, s, f;
};

struct SCC_TYPE {
  int fid, scc;
};

struct BCC_TYPE {
  int d, cid, p, dis, bcc;
};

struct BCC_2_TYPE {
  int d, cid, p, dis, nd, pre, oldc, oldd, minp, maxp, tmp;
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

struct HITS_TYPE {
  float auth, hub, auth1, hub1;
};
inline InArchive& operator<<(InArchive& in_archive, const HITS_TYPE& v) {
  in_archive << v.auth << v.hub;
  return in_archive;
}
inline OutArchive& operator>>(OutArchive& out_archive, HITS_TYPE& v) {
  out_archive >> v.auth >> v.hub;
  return out_archive;
}

struct MIS_TYPE {
  bool d, b;
  int64_t r;
};
inline InArchive& operator<<(InArchive& in_archive, const MIS_TYPE& v) {
  in_archive << v.d << v.r;
  return in_archive;
}
inline OutArchive& operator>>(OutArchive& out_archive, MIS_TYPE& v) {
  out_archive >> v.d >> v.r;
  return out_archive;
}

struct MIS_2_TYPE {
  bool d, b;
};

struct MM_TYPE {
  int32_t p, s;
};

struct MM_2_TYPE {
  int32_t p, s, d;
};

struct MIN_COVER_TYPE {
  bool c, s;
  int d, tmp;
};

struct MIN_COVER_TYPE_2 {
  bool c;
  int d, tmp, f;
};

struct MIN_DOMINATING_SET_TYPE {
  bool d, b;
  int max_id, max_cnt;
};

struct MIN_DOMINATING_SET_2_TYPE {
  bool d, b;
  int cnt, cnt1, fid1, fid2, tmp;
};

struct K_CORE_TYPE {
  int32_t d;
};

struct CORE_TYPE {
  short core;
  int cnt;
  std::vector<short> s;
};
inline InArchive& operator<<(InArchive& in_archive, const CORE_TYPE& v) {
  in_archive << v.core;
  return in_archive;
}
inline OutArchive& operator>>(OutArchive& out_archive, CORE_TYPE& v) {
  out_archive >> v.core;
  return out_archive;
}

struct CORE_2_TYPE {
  short core, old;
};
inline InArchive& operator<<(InArchive& in_archive, const CORE_2_TYPE& v) {
  in_archive << v.core;
  return in_archive;
}
inline OutArchive& operator>>(OutArchive& out_archive, CORE_2_TYPE& v) {
  out_archive >> v.core;
  return out_archive;
}

struct DEGENERACY_TYPE {
  short core, old;
  int d, tmp, rank;
};

struct AB_CORE_TYPE {
  int d, c;
};

struct ONION_TYPE {
  short core, old;
  int rank, tmp, d;
};

struct TRIANGLE_TYPE {
  int32_t deg, count;
  std::set<int32_t> out;
};
inline InArchive& operator<<(InArchive& in_archive, const TRIANGLE_TYPE& v) {
  in_archive << v.deg << v.out;
  return in_archive;
}
inline OutArchive& operator>>(OutArchive& out_archive, TRIANGLE_TYPE& v) {
  out_archive >> v.deg >> v.out;
  return out_archive;
}

struct RECTANGLE_TYPE {
  int32_t deg, count;
  std::vector<std::pair<int, int> > out;
};
inline InArchive& operator<<(InArchive& in_archive, const RECTANGLE_TYPE& v) {
  in_archive << v.deg << v.out;
  return in_archive;
}
inline OutArchive& operator>>(OutArchive& out_archive, RECTANGLE_TYPE& v) {
  out_archive >> v.deg >> v.out;
  return out_archive;
}

struct K_CLIQUE_2_TYPE {
  int32_t deg, count;
  std::vector<int32_t> out;
};
inline InArchive& operator<<(InArchive& in_archive, const K_CLIQUE_2_TYPE& v) {
  in_archive << v.deg << v.out;
  return in_archive;
}
inline OutArchive& operator>>(OutArchive& out_archive, K_CLIQUE_2_TYPE& v) {
  out_archive >> v.deg >> v.out;
  return out_archive;
}

struct MATRIX_TYPE {
  std::vector<float> val;
};
inline InArchive& operator<<(InArchive& in_archive, const MATRIX_TYPE& v) {
  in_archive << v.val;
  return in_archive;
}
inline OutArchive& operator>>(OutArchive& out_archive, MATRIX_TYPE& v) {
  out_archive >> v.val;
  return out_archive;
}

struct DENSEST_TYPE {
  short core, t;
};

struct COLOR_TYPE {
  short c, cc;
  int32_t deg;
  std::vector<int> colors;
};
inline InArchive& operator<<(InArchive& in_archive, const COLOR_TYPE& v) {
  in_archive << v.c << v.deg;
  return in_archive;
}
inline OutArchive& operator>>(OutArchive& out_archive, COLOR_TYPE& v) {
  out_archive >> v.c >> v.deg;
  return out_archive;
}

struct LPA_TYPE {
  int c, cc;
  std::vector<int> s;
};
inline InArchive& operator<<(InArchive& in_archive, const LPA_TYPE& v) {
  in_archive << v.c;
  return in_archive;
}
inline OutArchive& operator>>(OutArchive& out_archive, LPA_TYPE& v) {
  out_archive >> v.c;
  return out_archive;
}

struct LPA_BY_COLOR_TYPE {
  short c, cc;
  int deg, label, old, t;
  std::vector<int> colors;
};
inline InArchive& operator<<(InArchive& in_archive, const LPA_BY_COLOR_TYPE& v) {
  in_archive << v.c << v.deg << v.label << v.t;
  return in_archive;
}
inline OutArchive& operator>>(OutArchive& out_archive, LPA_BY_COLOR_TYPE& v) {
  out_archive >> v.c >> v.deg >> v.label >> v.t;
  return out_archive;
}

struct EGO_TYPE {
  int deg;
  std::vector<int> out;
  std::vector<std::vector<int> > ego;
};
inline InArchive& operator<<(InArchive& in_archive, const EGO_TYPE& v) {
  in_archive << v.deg << v.out;
  return in_archive;
}
inline OutArchive& operator>>(OutArchive& out_archive, EGO_TYPE& v) {
  out_archive >> v.deg >> v.out;
  return out_archive;
}

struct FLUID_TYPE {
  int lab, old, l1, l2;
};

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

  using GraphType =
        grape::ImmutableEdgecutFragment<int32_t, uint32_t, grape::EmptyType,
                                        grape::EmptyType,
                                        LoadStrategy::kBothOutIn>;
  using WeightedGraphType =
        grape::ImmutableEdgecutFragment<int32_t, uint32_t, grape::EmptyType,
                                        double,LoadStrategy::kBothOutIn>;                                    
  if (name == "bfs") {
    using AppType = grape::flash::BFSFlash<GraphType, BFS_TYPE>;
    CreateAndQuery<GraphType, AppType>(comm_spec, out_prefix, fnum, spec,
                                       FLAGS_bfs_source);
  } else if (name == "bfs-undirected") {
    using AppType = grape::flash::BFSUndirectedFlash<GraphType, BFS_TYPE>;
    CreateAndQuery<GraphType, AppType>(comm_spec, out_prefix, fnum, spec,
                                       FLAGS_bfs_source);
  } else if (name == "dfs") {
    using AppType = grape::flash::DFSFlash<GraphType, DFS_TYPE>;
    CreateAndQuery<GraphType, AppType>(comm_spec, out_prefix, fnum, spec);
  } else if (name == "dfs-undirected") {
    using AppType = grape::flash::DFSUndirectedFlash<GraphType, DFS_TYPE>;
    CreateAndQuery<GraphType, AppType>(comm_spec, out_prefix, fnum, spec);
  } else if (name == "sssp") {
    using AppType = grape::flash::SSSPFlash<WeightedGraphType, SSSP_TYPE>;
    CreateAndQuery<WeightedGraphType, AppType>(comm_spec, out_prefix, fnum, spec,
                                               FLAGS_sssp_source);
  } else if (name == "sssp-dlt-step") {
    using AppType = grape::flash::SSSPDltStepFlash<WeightedGraphType, SSSP_TYPE>;
    CreateAndQuery<WeightedGraphType, AppType>(comm_spec, out_prefix, fnum, spec,
                                               FLAGS_sssp_source);
  } else if (name == "random-walk") {
    using AppType = grape::flash::RandomWalkFlash<GraphType, RANDOM_WALK_TYPE>;
    CreateAndQuery<GraphType, AppType>(comm_spec, out_prefix, fnum, spec);
  } else if (name == "pagerank") {
    using AppType = grape::flash::PRFlash<GraphType, PR_TYPE>;
    CreateAndQuery<GraphType, AppType>(comm_spec, out_prefix, fnum, spec,
                                       FLAGS_pr_mr, FLAGS_pr_d);
  } else if (name == "article-rank") {
     using AppType = grape::flash::ArticleRankFlash<GraphType, PR_TYPE>;
     CreateAndQuery<GraphType, AppType>(comm_spec, out_prefix, fnum, spec,
                                       FLAGS_pr_mr, FLAGS_pr_d);
  } else if (name == "ppr") {
    using AppType = grape::flash::PPRFlash<GraphType, PR_TYPE>;
    CreateAndQuery<GraphType, AppType>(comm_spec, out_prefix, fnum, spec,
                                       FLAGS_ppr_s, FLAGS_pr_mr);
  } else if (name == "hits") {
    using AppType = grape::flash::HITSFlash<GraphType, HITS_TYPE>;
    CreateAndQuery<GraphType, AppType>(comm_spec, out_prefix, fnum, spec,
                                       FLAGS_pr_mr);
  } else if (name == "cc") {
    using AppType = grape::flash::CCFlash<GraphType, CC_TYPE>;
    CreateAndQuery<GraphType, AppType>(comm_spec, out_prefix, fnum, spec);
  } else if (name == "cc-opt") {
    using AppType = grape::flash::CCOptFlash<GraphType, CC_OPT_TYPE>;
    CreateAndQuery<GraphType, AppType>(comm_spec, out_prefix, fnum, spec);
  } else if (name == "cc-block") {
    using AppType = grape::flash::CCBlockFlash<GraphType, CC_TYPE>;
    CreateAndQuery<GraphType, AppType>(comm_spec, out_prefix, fnum, spec);
  } else if (name == "cc-union") {
    using AppType = grape::flash::CCUnionFlash<GraphType, CC_TYPE>;
    CreateAndQuery<GraphType, AppType>(comm_spec, out_prefix, fnum, spec);
  } else if (name == "cc-log") {
    using AppType = grape::flash::CCLogFlash<GraphType, CC_LOG_TYPE>;
    CreateAndQuery<GraphType, AppType>(comm_spec, out_prefix, fnum, spec);
  } else if (name == "scc") {
    using AppType = grape::flash::SCCFlash<GraphType, SCC_TYPE>;
    CreateAndQuery<GraphType, AppType>(comm_spec, out_prefix, fnum, spec);
  } else if (name == "scc-2") {
    using AppType = grape::flash::SCC2Flash<GraphType, SCC_TYPE>;
    CreateAndQuery<GraphType, AppType>(comm_spec, out_prefix, fnum, spec);
  } else if (name == "bcc") {
    using AppType = grape::flash::BCCFlash<GraphType, BCC_TYPE>;
    CreateAndQuery<GraphType, AppType>(comm_spec, out_prefix, fnum, spec);
  } else if (name == "bcc-2") {
    using AppType = grape::flash::BCC2Flash<GraphType, BCC_2_TYPE>;
    CreateAndQuery<GraphType, AppType>(comm_spec, out_prefix, fnum, spec);
  } else if (name == "cut-point") {
    using AppType = grape::flash::CutPointFlash<GraphType, BCC_TYPE>;
    CreateAndQuery<GraphType, AppType>(comm_spec, out_prefix, fnum, spec);
  } else if (name == "cut-point-2") {
    using AppType = grape::flash::CutPoint2Flash<GraphType, BCC_2_TYPE>;
    CreateAndQuery<GraphType, AppType>(comm_spec, out_prefix, fnum, spec);
  } else if (name == "bridge") {
    using AppType = grape::flash::BridgeFlash<GraphType, BCC_TYPE>;
    CreateAndQuery<GraphType, AppType>(comm_spec, out_prefix, fnum, spec);
  } else if (name == "bridge-2") {
    using AppType = grape::flash::Bridge2Flash<GraphType, BCC_2_TYPE>;
    CreateAndQuery<GraphType, AppType>(comm_spec, out_prefix, fnum, spec);
  } else if (name == "bc") {
    using AppType = grape::flash::BCFlash<GraphType, BC_TYPE>;
    CreateAndQuery<GraphType, AppType>(comm_spec, out_prefix, fnum, spec,
                                       FLAGS_bc_source);
  } else if (name == "bc-undirected") {
    using AppType = grape::flash::BCUndirectedFlash<GraphType, BC_TYPE>;
    CreateAndQuery<GraphType, AppType>(comm_spec, out_prefix, fnum, spec,
                                       FLAGS_bc_source);
  } else if (name == "katz") {
    using AppType = grape::flash::KATZFlash<GraphType, KATZ_TYPE>;
    CreateAndQuery<GraphType, AppType>(comm_spec, out_prefix, fnum, spec);
  } else if (name == "katz-undirected") {
    using AppType = grape::flash::KATZUndirectedFlash<GraphType, KATZ_TYPE>;
    CreateAndQuery<GraphType, AppType>(comm_spec, out_prefix, fnum, spec);
  } else if (name == "eigenvec") {
    using AppType = grape::flash::EigenvecFlash<GraphType, KATZ_TYPE>;
    CreateAndQuery<GraphType, AppType>(comm_spec, out_prefix, fnum, spec);
  } else if (name == "closeness") {
    using AppType = grape::flash::ClosenessFlash<GraphType, CLOSENESS_TYPE>;
    CreateAndQuery<GraphType, AppType>(comm_spec, out_prefix, fnum, spec);
  } else if (name == "harmonic") {
    using AppType = grape::flash::HarmonicFlash<GraphType, HARMONIC_TYPE>;
    CreateAndQuery<GraphType, AppType>(comm_spec, out_prefix, fnum, spec);
  } else if (name == "mis") {
    using AppType = grape::flash::MISFlash<GraphType, MIS_TYPE>;
    CreateAndQuery<GraphType, AppType>(comm_spec, out_prefix, fnum, spec);
  } else if (name == "mis-2") {
    using AppType = grape::flash::MIS2Flash<GraphType, MIS_2_TYPE>;
    CreateAndQuery<GraphType, AppType>(comm_spec, out_prefix, fnum, spec);
  } else if (name == "mm") {
    using AppType = grape::flash::MMFlash<GraphType, MM_TYPE>;
    CreateAndQuery<GraphType, AppType>(comm_spec, out_prefix, fnum, spec);
  } else if (name == "mm-opt") {
    using AppType = grape::flash::MMOptFlash<GraphType, MM_TYPE>;
    CreateAndQuery<GraphType, AppType>(comm_spec, out_prefix, fnum, spec);
  } else if (name == "mm-opt-2") {
    using AppType = grape::flash::MMOpt2Flash<GraphType, MM_2_TYPE>;
    CreateAndQuery<GraphType, AppType>(comm_spec, out_prefix, fnum, spec);
  } else if (name == "min-edge-cover") {
    using AppType = grape::flash::MinEdgeCoverFlash<GraphType, MM_2_TYPE>;
    CreateAndQuery<GraphType, AppType>(comm_spec, out_prefix, fnum, spec);
  } else if (name == "min-cover") {
    using AppType = grape::flash::MinCoverFlash<GraphType, MIN_COVER_TYPE>;
    CreateAndQuery<GraphType, AppType>(comm_spec, out_prefix, fnum, spec);
  } else if (name == "min-cover-greedy") {
    using AppType = grape::flash::MinCoverGreedyFlash<GraphType, MIN_COVER_TYPE>;
    CreateAndQuery<GraphType, AppType>(comm_spec, out_prefix, fnum, spec);
  } else if (name == "min-cover-greedy-2") {
    using AppType = grape::flash::MinCoverGreedy2Flash<GraphType, MIN_COVER_TYPE_2>;
    CreateAndQuery<GraphType, AppType>(comm_spec, out_prefix, fnum, spec);
  } else if (name == "min-dominating-set") {
    using AppType = grape::flash::MinDominatingSetFlash<GraphType, MIN_DOMINATING_SET_TYPE>;
    CreateAndQuery<GraphType, AppType>(comm_spec, out_prefix, fnum, spec);
  } else if (name == "min-dominating-set-2") {
    using AppType = grape::flash::MinDominatingSet2Flash<GraphType, MIN_DOMINATING_SET_2_TYPE>;
    CreateAndQuery<GraphType, AppType>(comm_spec, out_prefix, fnum, spec);
  } else if (name == "k-core-search") {
    using AppType = grape::flash::KCoreSearchFlash<GraphType, K_CORE_TYPE>;
    CreateAndQuery<GraphType, AppType>(comm_spec, out_prefix, fnum, spec,
                                       FLAGS_kc_k);
  } else if (name == "core") {
    using AppType = grape::flash::CoreFlash<GraphType, CORE_TYPE>;
    CreateAndQuery<GraphType, AppType>(comm_spec, out_prefix, fnum, spec);
  } else if (name == "core-2") {
    using AppType = grape::flash::Core2Flash<GraphType, CORE_2_TYPE>;
    CreateAndQuery<GraphType, AppType>(comm_spec, out_prefix, fnum, spec);
  } else if (name == "ab-core") {
    using AppType = grape::flash::ABCoreFlash<GraphType, AB_CORE_TYPE>;
    CreateAndQuery<GraphType, AppType>(comm_spec, out_prefix, fnum, spec,
                                       FLAGS_abcore_a, FLAGS_abcore_b, FLAGS_abcore_nx);
  } else if (name == "onion-layer-ordering") {
    using AppType = grape::flash::OnionFlash<GraphType, ONION_TYPE>;
    CreateAndQuery<GraphType, AppType>(comm_spec, out_prefix, fnum, spec);
  } else if (name == "degeneracy-ordering") {
    using AppType = grape::flash::DegeneracyFlash<GraphType, DEGENERACY_TYPE>;
    CreateAndQuery<GraphType, AppType>(comm_spec, out_prefix, fnum, spec);
  } else if (name == "color") {
    using AppType = grape::flash::ColorFlash<GraphType, COLOR_TYPE>;
    CreateAndQuery<GraphType, AppType>(comm_spec, out_prefix, fnum, spec);
  } else if (name == "lpa") {
    using LPAGraphType =
        grape::ImmutableEdgecutFragment<int32_t, uint32_t, uint32_t,
                                        grape::EmptyType,
                                        LoadStrategy::kBothOutIn>;
    using AppType = grape::flash::LPAFlash<LPAGraphType, LPA_TYPE>;
    CreateAndQuery<LPAGraphType, AppType>(comm_spec, out_prefix, fnum, spec);
  } else if (name == "lpa-by-color") {
    using AppType = grape::flash::LPAByColorFlash<GraphType, LPA_BY_COLOR_TYPE>;
    CreateAndQuery<GraphType, AppType>(comm_spec, out_prefix, fnum, spec);
  } else if (name == "ego-net") {
    using AppType = grape::flash::EgoFlash<GraphType, EGO_TYPE>;
    CreateAndQuery<GraphType, AppType>(comm_spec, out_prefix, fnum, spec);
  } else if (name == "clustering-coeff") {
    using CLUSTERTING_TYPE = K_CLIQUE_2_TYPE;
    using AppType = grape::flash::ClusteringCoeffFlash<GraphType, CLUSTERTING_TYPE>;
    CreateAndQuery<GraphType, AppType>(comm_spec, out_prefix, fnum, spec);
  } else if (name == "fluid-community") {
    using AppType = grape::flash::FluidCommunityFlash<GraphType, FLUID_TYPE>;
    CreateAndQuery<GraphType, AppType>(comm_spec, out_prefix, fnum, spec);
  } else if (name == "fluid-by-color") {
    using AppType = grape::flash::FluidByColorFlash<GraphType, LPA_BY_COLOR_TYPE>;
    CreateAndQuery<GraphType, AppType>(comm_spec, out_prefix, fnum, spec);
  } else if (name == "triangle") {
    using AppType = grape::flash::TriangleFlash<GraphType, TRIANGLE_TYPE>;
    CreateAndQuery<GraphType, AppType>(comm_spec, out_prefix, fnum, spec);
  } else if (name == "tailed-triangle") {
    using AppType = grape::flash::TailedTriangleFlash<GraphType, TRIANGLE_TYPE>;
    CreateAndQuery<GraphType, AppType>(comm_spec, out_prefix, fnum, spec);
  } else if (name == "3-path") {
    using AppType = grape::flash::ThreePathFlash<GraphType, TRIANGLE_TYPE>;
    CreateAndQuery<GraphType, AppType>(comm_spec, out_prefix, fnum, spec);
  } else if (name == "rectangle") {
    using AppType = grape::flash::RectangleFlash<GraphType, RECTANGLE_TYPE>;
    CreateAndQuery<GraphType, AppType>(comm_spec, out_prefix, fnum, spec);
  } else if (name == "diamond") {
    using AppType = grape::flash::DiamondFlash<GraphType, RECTANGLE_TYPE>;
    CreateAndQuery<GraphType, AppType>(comm_spec, out_prefix, fnum, spec);
  } else if (name == "k-clique") {
    using AppType = grape::flash::KCliqueFlash<GraphType, TRIANGLE_TYPE>;
    CreateAndQuery<GraphType, AppType>(comm_spec, out_prefix, fnum, spec,
                                       FLAGS_kcl_k);
  } else if (name == "k-clique-2") {
    using AppType = grape::flash::KClique2Flash<GraphType, K_CLIQUE_2_TYPE>;
    CreateAndQuery<GraphType, AppType>(comm_spec, out_prefix, fnum, spec,
                                       FLAGS_kcl_k);
  } else if (name == "matrix-fac") {
    using AppType = grape::flash::MatrixFacFlash<WeightedGraphType, MATRIX_TYPE>;
    CreateAndQuery<WeightedGraphType, AppType>(comm_spec, out_prefix, fnum, spec,
                                               FLAGS_mf_d);
  } else if (name == "densest-sub-2-approx") {
    using AppType = grape::flash::DensestFlash<GraphType, DENSEST_TYPE>;
    CreateAndQuery<GraphType, AppType>(comm_spec, out_prefix, fnum, spec);
  } else if (name == "msf") {
    using AppType = grape::flash::MSFFlash<WeightedGraphType, EMPTY_TYPE>;
    CreateAndQuery<WeightedGraphType, AppType>(comm_spec, out_prefix, fnum, spec);
  } else if (name == "msf-block") {
    using AppType = grape::flash::MSFBlockFlash<WeightedGraphType, EMPTY_TYPE>;
    CreateAndQuery<WeightedGraphType, AppType>(comm_spec, out_prefix, fnum, spec);
  } else if (name == "k-center") {
    using AppType = grape::flash::KCenterFlash<GraphType, BFS_TYPE>;
    CreateAndQuery<GraphType, AppType>(comm_spec, out_prefix, fnum, spec,
                                       FLAGS_kc_k);
  } else {
    LOG(FATAL) << "Invalid app name: " << name;
  }
}

}  // namespace flash
}  // namespace grape
#endif  // EXAMPLES_ANALYTICAL_APPS_RUN_FLASH_APP_H_
