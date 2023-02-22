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

#ifndef EXAMPLES_ANALYTICAL_APPS_FLASH_MIN_DOMINATING_SET_H_
#define EXAMPLES_ANALYTICAL_APPS_FLASH_MIN_DOMINATING_SET_H_

#include <grape/grape.h>

#include "../api.h"
#include "../flash_app_base.h"
#include "grape/fragment/immutable_edgecut_fragment.h"

namespace grape {
namespace flash {

template <typename FRAG_T, typename VALUE_T>
class MinDominatingSetFlash : public FlashAppBase<FRAG_T, VALUE_T> {
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

  bool* Res(value_t* v) { return &(v->b); }

  void Run(const fragment_t& graph) {
    int n_vertex = graph.GetTotalVerticesNum();
    Print("Run Min Dominating Set with Flash, total vertices: %d\n", n_vertex);

		DefineMapV(init) { v.max_cnt = Deg(id); v.d = false; v.b = false; v.max_id = id; };
		vset_t A = VertexMap(All, CTrueV, init);

		DefineMapV(local) {
			for_nb(
				if(!nb.d && (nb.max_cnt > v.max_cnt || (nb.max_cnt == v.max_cnt && nb.max_id > v.max_id))) { v.max_cnt = nb.max_cnt; v.max_id = nb.max_id; }
			)};

		for(int i = 0, len = VSize(A); len > 0; ++i, len = VSize(A)) {
			Print("Round %d,len=%d,", i, len);
			VertexMap(A, CTrueV, local);
			VertexMap(A, CTrueV, local);

			DefineFV(filter1) { return id == v.max_id; };
			DefineMapV(local1) { v.b = true; v.d = true; };
			vset_t B = VertexMap(A, filter1, local1);
			int cnt = VSize(B);
			Print("%d added\n", cnt);

			DefineFE(check) { return !d.d; };
			DefineMapE(update) { d.d = true; };
			EdgeMapSparse(B, EU, check, update, CTrueV, update);

			DefineFV(filter2) { return !v.d; };
			DefineMapV(local2) {
				v.max_id = id;
				v.max_cnt = 0;
				for_nb( if (!nb.d) ++v.max_cnt; );
			};
			A = VertexMap(A, filter2, local2);
		}

		DefineFV(filter) { return v.b; };
		int n_mc = VSize(VertexMap(All, filter));
		Print("size of min dominating set = %d\n", n_mc);
	}
   	
};

}  // namespace flash
}  // namespace grape

#endif  // EXAMPLES_ANALYTICAL_APPS_FLASH_MIN_DOMINATING_SET_H_
