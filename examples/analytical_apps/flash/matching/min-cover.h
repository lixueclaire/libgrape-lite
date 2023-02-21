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

#ifndef EXAMPLES_ANALYTICAL_APPS_FLASH_MIN_COVER_H_
#define EXAMPLES_ANALYTICAL_APPS_FLASH_MIN_COVER_H_

#include <grape/grape.h>

#include "../api.h"
#include "../flash_app_base.h"
#include "grape/fragment/immutable_edgecut_fragment.h"

namespace grape {
namespace flash {

template <typename FRAG_T, typename VALUE_T>
class MinCoverFlash : public FlashAppBase<FRAG_T, VALUE_T> {
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

		DefineMapV(init) { v.c = false; v.s = false; v.d = Deg(id); v.tmp = 0; };
		vset_t A = VertexMap(All, CTrueV, init);

		for (int i = 0, len = n_vertex, nowd = n_vertex / 2; len > 0; len = VSize(A), ++i, nowd /= 2) {
			Print("Round %d: size=%d\n", i, len);

			DefineFV(filter1) { return v.d >= nowd; };
			DefineMapV(local1) { v.c = true; };
			vset_t B = VertexMap(A, filter1, local1);

			DefineMapE(update) { d.tmp ++; };
			DefineMapE(reduce) { d.tmp += s.tmp; };
			B = EdgeMapSparse(B, EU, CTrueE, update, CTrueV, reduce);

			DefineMapV(local2) { v.d -= v.tmp; v.tmp = 0; };
			VertexMap(B, CTrueV, local2);

			DefineFV(filter2) {return !v.c && v.d > 0; };
			A = VertexMap(A, filter2);
		}

		DefineFV(filter) { return v.c; };
		for (int len = n_vertex, i=0; len > 0; ++i) {
			A = VertexMap(All, filter); 

			DefineFV(filter2) {
				for_nb(if (!nb.c) return false;);
				return true;
			};
			DefineMapV(local2) { v.s = true; };
			vset_t B = VertexMap(A, filter2, local2);

			DefineFV(filter3) { 
				if (!v.s) return false;
				for_nb(if(nb.s && nb_id > id) return false;);
				return true;
			};
			DefineMapV(local3) { v.c = false; };
			A = VertexMap(A, filter3, local3);
			len = VSize(A);

			DefineMapV(reset) { v.s = false; };
			VertexMap(B, CTrueV, reset);
			Print("Refining round %d: len=%d\n", i+1, len);
		}

		int n_mc = VSize(VertexMap(All, filter));
		Print("size of vertex-cover=%d\n", n_mc);
	}
   	
};

}  // namespace flash
}  // namespace grape

#endif  // EXAMPLES_ANALYTICAL_APPS_FLASH_MIN_COVER_H_
