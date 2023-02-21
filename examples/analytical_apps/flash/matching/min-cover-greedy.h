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

#ifndef EXAMPLES_ANALYTICAL_APPS_FLASH_MIN_COVER_GREEDY_H_
#define EXAMPLES_ANALYTICAL_APPS_FLASH_MIN_COVER_GREEDY_H_

#include <grape/grape.h>

#include "../api.h"
#include "../flash_app_base.h"
#include "grape/fragment/immutable_edgecut_fragment.h"

namespace grape {
namespace flash {

template <typename FRAG_T, typename VALUE_T>
class MinCoverGreedyFlash : public FlashAppBase<FRAG_T, VALUE_T> {
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

  bool* Res(value_t* v) { return &(v->c); }

  void Run(const fragment_t& graph) {
    int n_vertex = graph.GetTotalVerticesNum();
    Print("Run Min Cover with Flash, total vertices: %d\n", n_vertex);

		DefineMapV(init) { v.c = false; v.d = Deg(id); v.tmp = 0; };
		vset_t A = VertexMap(All, CTrueV, init);

		for(int i = 0, len = VSize(A); len > 0; ++i, len = VSize(A)) {
			Print("Round %d,len=%d,", i, len);

			DefineFV(filter1) {
				for_nb(if (!nb.c && (nb.d > v.d || (nb.d == v.d && nb_id > id))) return false;);
				return true;
			};
			DefineMapV(local1) { v.c = true; };
			vset_t B = VertexMap(A, filter1, local1);
			int cnt = VSize(B);
			Print("selected=%d\n", cnt);

			DefineFE(check) { return !d.c; };
			DefineMapE(update) { d.tmp ++; };
			DefineMapE(reduce) { d.tmp += s.tmp; };
			B = EdgeMapSparse(B, EU, check, update, CTrueV, reduce);

			DefineMapV(local2) { v.d -= v.tmp; v.tmp = 0; };
			VertexMap(B, CTrueV, local2);

			DefineFV(filter2) {return !v.c && v.d > 0; };
			A = VertexMap(A, filter2);
		}

		DefineFV(filter) { return v.c; };
		int n_mc = VSize(VertexMap(All, filter));
		Print("size of vertex-cover=%d\n", n_mc);
	}
   	
};

}  // namespace flash
}  // namespace grape

#endif  // EXAMPLES_ANALYTICAL_APPS_FLASH_MIN_COVER_GREEDY_H_
