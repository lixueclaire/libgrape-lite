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

#ifndef EXAMPLES_ANALYTICAL_APPS_FLASH_PPR_H_
#define EXAMPLES_ANALYTICAL_APPS_FLASH_PPR_H_

#include <grape/grape.h>

#include "../api.h"
#include "../flash_app_base.h"
#include "grape/fragment/immutable_edgecut_fragment.h"

namespace grape {
namespace flash {

template <typename FRAG_T, typename VALUE_T>
class PPRFlash : public FlashAppBase<FRAG_T, VALUE_T> {
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

  float* Res(value_t* v) { return &(v->val); };

  void Run(const fragment_t& graph, int source, int max_iters) {
    Print("Run PPR with Flash, max_iters = %d\n", max_iters);
    int n_vertex = graph.GetTotalVerticesNum();
    Print("Total vertices: %d, source = %d\n", n_vertex, source);

    vid_t gid;
    graph.GetVertexMap()->GetGid(source, gid);
    source = All.fw->Gid2Key(gid);

    DefineMapV(init_v) {
      v.val = 0;
      v.next = (id == source ? 0.5: 0);
      v.deg = Deg(id);
    };
    VertexMap(All, CTrueV, init_v);
    Print("Init complete\n");

    DefineFV(filter) { return id == source; };
    DefineMapV(local) { v.val = 1.0f; };
    vset_t A = VertexMap(All, filter, local);

	  for(int i = 0; i < max_iters; i++) {
		  Print("Round %d\n", i);
		  DefineMapE(update) { d.next += 0.5 * s.val / s.deg; };
		  A = EdgeMapDense(All, EU, CTrueE, update, CTrueV, false);

		  DefineMapV(local2) { v.val = v.next; v.next = (id == source ? 0.5 : 0); };
		  A = VertexMap(All, CTrueV, local2);
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

#endif  // EXAMPLES_ANALYTICAL_APPS_FLASH_PPR_H_
