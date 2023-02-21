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

#ifndef EXAMPLES_ANALYTICAL_APPS_FLASH_MIN_EDGE_COVER_H_
#define EXAMPLES_ANALYTICAL_APPS_FLASH_MIN_EDGE_COVER_H_

#include <grape/grape.h>

#include "../api.h"
#include "../flash_app_base.h"
#include "grape/fragment/immutable_edgecut_fragment.h"

namespace grape {
namespace flash {

template <typename FRAG_T, typename VALUE_T>
class MinEdgeCoverFlash : public FlashAppBase<FRAG_T, VALUE_T> {
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

  int32_t* Res(value_t* v) { return &(v->s); }

  void Run(const fragment_t& graph) {
    int n_vertex = graph.GetTotalVerticesNum();
    Print("Run Min Edge Cover with Flash, total vertices: %d\n", n_vertex);

		DefineMapV(init) { v.s = -1; v.p = -1; v.d = Deg(id); };
		vset_t A = VertexMap(All, CTrueV, init);

		DefineMapV(local) {
			v.p = -1; 
			int d; 
			for_nb (
				if (nb.s == -1) 
				if (v.p == -1 || nb.d < d || (nb.d == d && nb_id < v.p)) { d = nb.d; v.p = nb_id; }
			)
		};
		DefineFV(filter1) { return v.p >= 0; };

		DefineOutEdges(edges) { VjoinP(p); };
		DefineFE(check2) { return s.p != -1 && d.p == sid; };
		DefineMapE(update2) { d.s = d.p; };
		DefineFV(cond) { return v.s == -1; };

		DefineFE(check3) { return d.p == sid; };
		DefineMapE(update3) { d.p = -1; };

		for(int i=0, len = VSize(A); len > 0; ++i, len = VSize(A)) {
			Print("Round %d: size=%d\n", i, len);

			A = VertexMap(A, CTrueV, local);
			A = VertexMap(A, filter1);
			A = EdgeMapSparse(A, edges, check2, update2, cond);
			vset_t B = EdgeMapSparse(A, edges, check2, update2, cond);

			A = A.Union(B);
			A = EdgeMapSparse(A, EU, check3, update3, cond);
		}

		DefineFV(filter2) { return v.s >= 0; };
		int n_match = VSize(VertexMap(All, filter2))/2;
		DefineFV(filter3) { return v.s == -1 && v.d > 0; };
		n_match += VSize(VertexMap(All, filter3));
	  Print("number of edges=%d\n", n_match);
	}
   	
};

}  // namespace flash
}  // namespace grape

#endif  // EXAMPLES_ANALYTICAL_APPS_FLASH_MIN_EDGE_COVER_H_
