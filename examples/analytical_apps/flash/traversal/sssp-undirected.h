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

#ifndef EXAMPLES_ANALYTICAL_APPS_FLASH_SSSP_UNDIRECTED_H_
#define EXAMPLES_ANALYTICAL_APPS_FLASH_SSSP_UNDIRECTED_H_

#include <grape/grape.h>

#include "../api.h"
#include "../flash_app_base.h"
#include "grape/fragment/immutable_edgecut_fragment.h"

namespace grape {
namespace flash {

template <typename FRAG_T, typename VALUE_T>
class SSSPUndirectedFlash : public FlashAppBase<FRAG_T, VALUE_T> {
 public:
  using fragment_t = FRAG_T;
  using vid_t = typename fragment_t::vid_t;
  using vertex_t = typename fragment_t::vertex_t;
  using value_t = VALUE_T;
  using vdata_t = typename fragment_t::vdata_t;
  using edata_t = typename fragment_t::edata_t;
  using adj_list_t = typename fragment_t::adj_list_t;
  using vset_t = VertexSubset<fragment_t, value_t>;
  bool sync_all_ = false;

  float* Res(value_t* v) { return &(v->dis); }

  void Run(const fragment_t& graph, vid_t source) {
    Print("Run undirected SSSP with Flash, source = %d\n", source);
    int n_vertex = graph.GetTotalVerticesNum();
    Print("Total vertices: %d\n", n_vertex);
    vset_t a = All;
    vid_t gid;
    graph.GetVertexMap()->GetGid(source, gid);
    source = All.fw->Gid2Key(gid);

    DefineMapV(init_v) { v.dis = (id == source) ? 0 : -1; };
    a = VertexMap(a, CTrueV, init_v);

    DefineFV(f_filter) { return id == source; };
    a = VertexMap(a, f_filter);

    DefineFE(check) { return (d.dis < -0.5 || d.dis > s.dis + weight); };
    DefineMapE(update) { if (d.dis < -0.5 || d.dis > s.dis + weight) d.dis = s.dis + weight; };
    DefineMapE(reduce) { if (d.dis < -0.5 || d.dis > s.dis) d.dis = s.dis; };

    for (int len = VSize(a), i = 1; len > 0; len = VSize(a), ++i) {
      Print("Round %d: size = %d\n", i, len);
      a = EdgeMap(a, EU, check, update, CTrueV, reduce);
    }
  }
};

}  // namespace flash
}  // namespace grape

#endif  // EXAMPLES_ANALYTICAL_APPS_FLASH_SSSP_UNDIRECTED_H_
