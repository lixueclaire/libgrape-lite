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

#ifndef EXAMPLES_ANALYTICAL_APPS_FLASH_PAGERANK_H_
#define EXAMPLES_ANALYTICAL_APPS_FLASH_PAGERANK_H_

#include <grape/grape.h>

#include "../api.h"
#include "../flash_app_base.h"
#include "grape/fragment/immutable_edgecut_fragment.h"

namespace grape {
namespace flash {

template <typename FRAG_T, typename VALUE_T>
class PRFlash : public FlashAppBase<FRAG_T, VALUE_T> {
 public:
  using fragment_t = FRAG_T;
  using vid_t = typename fragment_t::vid_t;
  using vertex_t = typename fragment_t::vertex_t;
  using value_t = VALUE_T;
  using adj_list_t = typename fragment_t::adj_list_t;
  using vertex_subset = VertexSubset<fragment_t, value_t>;
  bool sync_all_ = false;

  float* Res(value_t* v) { return &(v->val); };

  void Run(const fragment_t& graph, int max_iters, float damping = 0.85) {
    std::cout << "Run PageRank with Flash, max_iters = " << max_iters << std::endl;
    int n_vertex = graph.GetTotalVerticesNum();
    std::cout << "Total vertices: " << n_vertex << std::endl;

    DefineMapV(init_v) {
      v.val = 1.0 / n_vertex;
      v.next = 0;
      v.deg = OutDeg(id);
    };
    VertexMap(All, CTrueV, init_v);
    std::cout << "Init complete" << std::endl;

    DefineMapE(update) { d.next += damping * s.val / s.deg; };
    DefineMapV(local) {
      v.val = v.next + (1 - damping) / n_vertex +
              (v.deg == 0 ? damping * v.val : 0);
      v.next = 0;
    };

    for (int i = 0; i < max_iters; i++) {
      if (All.fw->GetPid() == 0)
        printf("Round %d\n", i);
      EdgeMapDense(All, ED, CTrueE, update, CTrueV);
      VertexMap(All, CTrueV, local);
    }

    /*double loc_val = 0, tot_val = 0;
    for (auto& i : *(All.fw->GetMasters()))
      loc_val += GetV(i)->val;
    All.fw->Sum(loc_val, tot_val);
    std::cout << "total = " << tot_val << std::endl;*/
  }
};

}  // namespace flash
}  // namespace grape

#endif  // EXAMPLES_ANALYTICAL_APPS_FLASH_PAGERANK_H_
