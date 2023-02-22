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

#ifndef EXAMPLES_ANALYTICAL_APPS_FLASH_MIN_DOMINATING_SET_2_H_
#define EXAMPLES_ANALYTICAL_APPS_FLASH_MIN_DOMINATING_SET_2_H_

#include <grape/grape.h>

#include "../api.h"
#include "../flash_app_base.h"
#include "grape/fragment/immutable_edgecut_fragment.h"

namespace grape {
namespace flash {

template <typename FRAG_T, typename VALUE_T>
class MinDominatingSet2Flash : public FlashAppBase<FRAG_T, VALUE_T> {
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

		DefineMapV(init) { v.cnt = Deg(id); v.d = false; v.b = false; v.tmp = 0; };
		vset_t A = VertexMap(All, CTrueV, init);

		DefineMapV(hop1) {
			v.fid1 = id;
			v.cnt1 = v.cnt;
			for_nb(if(!nb.d && (nb.cnt > v.cnt1 || (nb.cnt == v.cnt1 && nb_id > v.fid1))){ v.cnt1 = nb.cnt; v.fid1 = nb_id;})
		};
		DefineMapV(hop2) {
 			int cnt2 = v.cnt; 
			int mid2 = id; 
			v.fid2 = id; 
			for_nb(if(!nb.d && (nb.cnt1 > cnt2 || (nb.cnt1 == cnt2 && nb.fid1 > mid2))){ cnt2 = nb.cnt1; mid2 = nb.fid1; v.fid2 = nb_id;})
		};
		VertexMap(A, CTrueV, hop1);
		VertexMap(A, CTrueV, hop2);

		for(int i = 0, len = VSize(A); len > 0; ++i, len = VSize(A)) {
			DefineFV(filter1) { return id == v.fid2; };
			DefineMapV(local1) { v.b = true; v.d = true; };
			A = VertexMap(A, filter1, local1);
			int cnt = VSize(A);
			Print("Round %d, len=%d, %d added\n", i, len, cnt);

			DefineFE(check) { return !d.d; };
			DefineMapE(update) { d.d = true; };
			A = EdgeMapSparse(A, EU, check, update, CTrueV, update);

			DefineMapE(update2) { d.tmp++; };
			DefineMapE(reduce) { d.tmp += s.tmp; };
			A = EdgeMapSparse(A, EU, check, update2, CTrueV, reduce);

			DefineMapV(local2) { v.cnt -= v.tmp; v.tmp = 0; };
			A = VertexMap(A, CTrueV, local2);

			DefineFE(check2) { return !d.d && d.fid1 == sid; };
			DefineFE(check3) { return !d.d && d.fid2 == sid; };
			DefineMapE(none) {};
			vset_t B = A.Union(EdgeMapSparse(A, EU, check2, none, CTrueV));
			VertexMap(B, CTrueV, hop1);
			A = A.Union(EdgeMapSparse(B, EU, check3, none, CTrueV));
			VertexMap(A, CTrueV, hop2);
		}

		DefineFV(filter) { return v.b; };
		int n_mc = VSize(VertexMap(All, filter));
		Print("size of min dominating set = %d\n", n_mc);
	}
   	
};

}  // namespace flash
}  // namespace grape

#endif  // EXAMPLES_ANALYTICAL_APPS_FLASH_MIN_DOMINATING_SET_2_H_
