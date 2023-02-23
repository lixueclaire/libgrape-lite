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

#ifndef EXAMPLES_ANALYTICAL_APPS_FLASH_BCC_2_H_
#define EXAMPLES_ANALYTICAL_APPS_FLASH_BCC_2_H_

#include <grape/grape.h>

#include "../api.h"
#include "../flash_app_base.h"
#include "grape/fragment/immutable_edgecut_fragment.h"

#define GT(A,B) (A.d>B.d || (A.d==B.d && A.cid>B.cid))

namespace grape {
namespace flash {

template <typename FRAG_T, typename VALUE_T>
class BCC2Flash : public FlashAppBase<FRAG_T, VALUE_T> {
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

  int* Res(value_t* v) { return &(v->cid); }

  void Run(const fragment_t& graph) {
    int n_vertex = graph.GetTotalVerticesNum();
    Print("Run BCC with Flash, total vertices: %d\n", n_vertex);

    DefineMapV(init) { v.cid = id; v.d = Deg(id); v.dis = -1; v.tmp = 0; };
	  vset_t A = VertexMap(All, CTrueV, init);

	  for(int len = VSize(A), i = 0; len > 0; len = VSize(A), ++i) {
		  Print("CC Round %d: size=%d\n", i, len);

      DefineFE(check1) { return GT(s, d); };
		  DefineMapE(update1) { d.cid = s.cid; d.d = s.d; };
		  DefineMapE(reduce1) { if (GT(s, d)) { d.cid = s.cid; d.d = s.d; } };
		  A = EdgeMap(A, EU, check1, update1, CTrueV, reduce1);
	  }

	  std::vector<vset_t> v_bfs;

	  DefineFV(filter1) { return v.cid == id; };
	  DefineMapV(local1) { v.dis = 0; };
	  A = VertexMap(All, filter1, local1);

	  for(int len = VSize(A), i = 1; len > 0; len = VSize(A), ++i) {
		  Print("BFS Round %d: size=%d\n", i, len);
		  v_bfs.push_back(A);

		  DefineMapE(update2) { d.dis = i; };
		  DefineFV(cond2) { return (v.dis == -1); };
		  A = EdgeMap(A, EU, CTrueE, update2, cond2, update2);
	  }

		Print( "Computing Pre-Order...\n" );
		DefineMapV(pre_init) {
			v.nd = 1; v.p = -1; v.pre = 0;
			for_nb(if (nb.dis == v.dis - 1) {v.p = nb_id; break; });
		};
		VertexMap(All, CTrueV, pre_init);

		for(int i = (int)v_bfs.size() - 1; i >= 0; --i) {
			DefineFV(filter) { return v.p >= 0; };
			vset_t B = VertexMap(v_bfs[i], filter);

			DefineOutEdges(edges) { VjoinP(p) };
			DefineMapE(update) { d.tmp += s.nd; };
			DefineMapE(reduce) { d.tmp += s.tmp; };
			B = EdgeMapSparse(B, edges, CTrueE, update, CTrueV, reduce);

			DefineMapV(local) { v.nd += v.tmp; v.tmp = 0; };
			B = VertexMap(B, CTrueV, local);
		}

		for(int i = 0; i < (int)v_bfs.size(); ++i) {
			int ss = -1, pre = 0;
			DefineFE(check) { return d.p == sid; };
			DefineMapE(update) { if (sid != ss) pre = s.pre +1; ss = sid; d.pre = pre; pre += d.nd; };
			DefineMapE(reduce) { d.pre = s.pre; };
			EdgeMapSparse(v_bfs[i], EU, check, update, CTrueV, reduce);
		}

		Print("Computing minp and maxp...\n");
		DefineMapV(mm_init) { v.minp = v.pre; v.maxp = v.pre; };
		VertexMap(All, CTrueV, mm_init);
		for(int i = (int)v_bfs.size() - 1; i >= 0; --i) {
			DefineMapV(compute_mm) {
				for_nb( if(nb.p == id) {v.minp = std::min(v.minp, nb.minp); v.maxp = std::max(v.maxp, nb.maxp); } else if (v.p != nb_id) {v.minp = std::min(v.minp,nb.pre); v.maxp = std::max(v.maxp, nb.pre);})
			};
			VertexMap(v_bfs[i], CTrueV, compute_mm);
		}

		DefineMapV(bcc) {
			v.oldc = v.cid;
			v.oldd = v.d;
			for_nb(
				if (GT(v, nb)) continue;
				if (v.p == nb_id) {if(v.minp < nb.pre || v.maxp >= nb.pre+nb.nd) {v.cid=nb.cid;v.d=nb.d;}}
				else if(id == nb.p) {if(nb.minp < v.pre || nb.maxp >= v.pre+v.nd) {v.cid=nb.cid;v.d=nb.d;}}
				else {v.cid = nb.cid; v.d = nb.d;})
		};
		DefineMapV(init_c) { v.cid = id; v.d = Deg(id); };
		A = VertexMap(All, CTrueV, init_c);
		for(int len = VSize(A), i = 0; len > 0; len = VSize(A), ++i) {
			Print("BCC Round %d: size=%d\n", i, len);
			A = VertexMap(All, CTrueV, bcc, false);

			DefineFV(filter) { return v.oldc != v.cid || v.oldd != v.d; };
			DefineMapV(local) { v.oldc = v.cid; v.oldd = v.d; };
			A = VertexMap(A, filter, local);
		}
  }
};

}  // namespace flash
}  // namespace grape

#endif  // EXAMPLES_ANALYTICAL_APPS_FLASH_BCC_2_H_
