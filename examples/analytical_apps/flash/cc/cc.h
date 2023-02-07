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

#ifndef EXAMPLES_ANALYTICAL_APPS_FLASH_CC_H_
#define EXAMPLES_ANALYTICAL_APPS_FLASH_CC_H_

#include <grape/grape.h>

#include "../api.h"
#include "../flash_app_base.h"
#include "grape/fragment/immutable_edgecut_fragment.h"

namespace grape {
namespace flash {

template <typename FRAG_T, typename VALUE_T>
class CCFlash : public FlashAppBase<FRAG_T, VALUE_T> {
 public:
  using fragment_t = FRAG_T;
  using vid_t = typename fragment_t::vid_t;
  using vertex_t = typename fragment_t::vertex_t;
  using value_t = VALUE_T;
  using adj_list_t = typename fragment_t::adj_list_t;
  using vset_t = VertexSubset<fragment_t, value_t>;
  bool sync_all_ = false;

  int* Res(value_t* v) { return &(v->tag); }

  void Run(const fragment_t& graph) {
    int n_vertex = graph.GetTotalVerticesNum();
    Print("Run CC with Flash, total vertices: %d\n", n_vertex);
    vset_t a = All;

    DefineMapV(init_v) { v.tag = id; };
    a = VertexMap(a, CTrueV, init_v);
    // std::cout << "-Local Init: " << a.fw->GetPid() << ' ' << a.s.size() << ' '
    //          << VSize(a) << std::endl;

    DefineFE(check) { return s.tag < d.tag; };
    DefineMapE(update) { d.tag = std::min(s.tag, d.tag); };

    for (int len = VSize(a), i = 1; len > 0; len = VSize(a), ++i) {
      // Print("Round %d (Dense): size = %d\n", i, len);
      // a = EdgeMapDense(a, EU, check, update, CTrueV);

      // Print("Round %d (Sparse): size = %d\n", i, len);
      // a = EdgeMapSparse(a, EU, check, update, CTrueV, update);

      Print("Round %d: size = %d\n", i, len);
      a = EdgeMap(a, EU, check, update, CTrueV, update);
    }
  }
};

}  // namespace flash
}  // namespace grape

#endif  // EXAMPLES_ANALYTICAL_APPS_FLASH_BFS_H_
