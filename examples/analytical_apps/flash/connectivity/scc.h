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

#ifndef EXAMPLES_ANALYTICAL_APPS_FLASH_SCC_H_
#define EXAMPLES_ANALYTICAL_APPS_FLASH_SCC_H_

#include <grape/grape.h>

#include "../api.h"
#include "../flash_app_base.h"
#include "grape/fragment/immutable_edgecut_fragment.h"

namespace grape {
namespace flash {

template <typename FRAG_T, typename VALUE_T>
class SCCFlash : public FlashAppBase<FRAG_T, VALUE_T> {
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

  int* Res(value_t* v) { return &(v->scc); }

  void Run(const fragment_t& graph) {
    int n_vertex = graph.GetTotalVerticesNum();
    Print("Run SCC with Flash, total vertices: %d\n", n_vertex);

    DefineMapV(init) { v.scc = -1; };
	  vset_t A = VertexMap(All, CTrueV, init);

	  DefineMapV(local1) { v.fid = id; };

	  DefineFV(filter2) { return v.fid == id; };
	  DefineMapV(local2) { v.scc = id; };

	  DefineFV(filter3) { return v.scc == -1; };
	
	 for (int i = 1, vsa = VSize(A); vsa > 0; vsa = VSize(A), ++i) {
		  vset_t B = VertexMap(A, CTrueV, local1);
		  for (int vsb = VSize(B), j = 1; vsb > 0; vsb = VSize(B), ++j) {
			  Print("Round %d.1.%d: na=%d, nb=%d\n", i, j, vsa, vsb );

        DefineFE(check1) { return s.fid < d.fid; };
			  DefineMapE(update1) { d.fid = std::min(d.fid, s.fid); };
			  DefineFV(cond1) { return v.scc == -1; };

			  B = EdgeMap(B, EjoinV(ED, A), check1, update1, cond1);
		  }

		  B = VertexMap(A, filter2, local2);

		  for(int vsb = VSize(B), j = 1; vsb > 0; vsb = VSize(B), ++j) {
			  Print("Round %d.2.%d: na=%d, nb=%d\n", i, j, vsa, vsb );

			  DefineFE(check2) { return s.scc == d.fid; };
			  DefineMapE(update2) { d.scc = d.fid; };
			  DefineFV(cond2) { return v.scc == -1; };
			
			  B = EdgeMap(B, EjoinV(ER, A), check2, update2, cond2);
		  }

		  A = VertexMap(A, filter3);
	  }
  }
};

}  // namespace flash
}  // namespace grape

#endif  // EXAMPLES_ANALYTICAL_APPS_FLASH_SCC_H_
