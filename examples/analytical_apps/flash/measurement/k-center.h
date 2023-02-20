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

#ifndef EXAMPLES_ANALYTICAL_APPS_FLASH_K_CENTER_H_
#define EXAMPLES_ANALYTICAL_APPS_FLASH_K_CENTER_H_

#include <grape/grape.h>

#include "../api.h"
#include "../flash_app_base.h"
#include "grape/fragment/immutable_edgecut_fragment.h"

namespace grape {
namespace flash {

template <typename FRAG_T, typename VALUE_T>
class KCenterFlash : public FlashAppBase<FRAG_T, VALUE_T> {
 public:
  using fragment_t = FRAG_T;
  using vid_t = typename fragment_t::vid_t;
  using vertex_t = typename fragment_t::vertex_t;
  using value_t = VALUE_T;
  using vdata_t = typename fragment_t::vdata_t;
  using edata_t = typename fragment_t::edata_t;
  using adj_list_t = typename fragment_t::adj_list_t;
  using vset_t = VertexSubset<fragment_t, value_t>;
  using E = std::pair<edata_t, std::pair<vid_t, vid_t> >;
  bool sync_all_ = false;
  int* Res(value_t* v) { return &(v->dis); }

  void Run(const fragment_t& graph, int k) {
    Print("Run K-center with Flash, k = %d, ", k);
    int n_vertex = graph.GetTotalVerticesNum();
    Print("total vertices: %d\n", n_vertex);

    std::pair<int, int> v_loc = std::make_pair(0, 0), v_glb;
    DefineMapV(init) {
      if (Deg(id) > v_loc.first) 
        v_loc = std::make_pair(Deg(id), id);
      v.dis = INT_MAX;
    };
    VertexMapSeq(All, CTrueV, init);

	  for(int i = 0; i < k; ++i) {
		  GetMax(v_loc, v_glb);
		  int sid = v_glb.second;
		  Print( "Round %d: max_min_dis=%d\n", i, v_glb.first);

      DefineFV(filter) { return id == sid; };
      DefineMapV(local) { v.dis = 0; };
      vset_t A = VertexMap(All, filter, local);

		  for(int len = A.size(), j = 1; len > 0; len = A.size(), ++j) {
        DefineFE(check) { return d.dis > j; };
        DefineMapE(update) { d.dis = j; };
        A = EdgeMapSparse(A, EU, check, update, CTrueV, update);
      }

		  v_loc = std::make_pair(0, 0);
      for (auto &id: All.s) {
        if (GetV(id)->dis > v_loc.first) 
          v_loc = std::make_pair(GetV(id)->dis, id);
      }
	  }
  }
};

}  // namespace flash
}  // namespace grape

#endif  // EXAMPLES_ANALYTICAL_APPS_FLASH_K_CENTER_H_
