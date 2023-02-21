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

#ifndef EXAMPLES_ANALYTICAL_APPS_FLASH_CUT_POINT_H_
#define EXAMPLES_ANALYTICAL_APPS_FLASH_CUT_POINT_H_

#include <grape/grape.h>

#include "../api.h"
#include "../flash_app_base.h"
#include "grape/fragment/immutable_edgecut_fragment.h"

#define GT(A,B) (A.d>B.d || (A.d==B.d && A.cid>B.cid))

namespace grape {
namespace flash {

template <typename FRAG_T, typename VALUE_T>
class CutPointFlash : public FlashAppBase<FRAG_T, VALUE_T> {
 public:
  using fragment_t = FRAG_T;
  using vid_t = typename fragment_t::vid_t;
  using vertex_t = typename fragment_t::vertex_t;
  using value_t = VALUE_T;
  using vdata_t = typename fragment_t::vdata_t;
  using edata_t = typename fragment_t::edata_t;
  using adj_list_t = typename fragment_t::adj_list_t;
  using vset_t = VertexSubset<fragment_t, value_t>;
  bool sync_all_ = true;

  int* Res(value_t* v) { return &(v->bcc); }

  void Run(const fragment_t& graph) {
    int n_vertex = graph.GetTotalVerticesNum();
    Print("Run cut point detection with Flash, total vertices: %d\n", n_vertex);

    DefineMapV(init) { v.cid = id; v.d = Deg(id); v.dis = -1; v.p = -1;  };
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

    DefineMapV(local) {
      v.p = -1;
      for_nb( if (nb.dis == v.dis-1) { v.p = nb_id; break; });
    };
    VertexMap(All, CTrueV, local);

	  DefineFE(check3) { return (s.dis == d.dis - 1); };
	  DefineMapE(update3) { d.p = sid; };
	  DefineFV(cond3) {return (v.p == -1);};
	  DefineMapE(reduce3) { d = s; };
	  EdgeMap(All, EU, check3, update3, cond3, reduce3);

	  Print( "Joining Edges...\n" );
	  union_find f(n_vertex), cc;

	  DefineMapV(join_edges) {
		  for_nb(
        if (nb_id > id && v.p != nb_id && nb.p != id) {
			    int a = nb_id, b = id;
			    union_f(f, a, b);
			    while (a != b) {
				    int da = GetV(a)->dis, db = GetV(b)->dis, pa = GetV(a)->p, pb = GetV(b)->p;
				    if (da >= db) {if (pa != pb) union_f(f, pa, a); a = pa;}
				    if (db >= da) {if (pa != pb) union_f(f, pb, b); b = pb;}
			    }
		    });
	  };
	  VertexMapSeq(All, CTrueV, join_edges, false);

	  Print("Reducing...\n" );
	  Reduce(f, cc, for_i(union_f(cc, f[i], i)), true);

    for (int i = 0; i < n_vertex; ++i) {
		  int fi = get_f(cc, cc[i]);
		  cc[i] = fi;
	  }

    DefineMapV(final) {
			v.d = 0;
			int c = v.p==-1?-1:cc[id];
			for_nb(if (nb.p == id) {if (c == -1) c = cc[nb_id]; else if (cc[nb_id] != c) v.bcc = 1;})
		};
    VertexMap(All, CTrueV, final, false);
		DefineFV(filter) { return v.bcc; };
		int cnt = VSize(VertexMap(All, filter));
		Print("num_cut_point=%d\n", cnt);
  }
};

}  // namespace flash
}  // namespace grape

#endif  // EXAMPLES_ANALYTICAL_APPS_FLASH_CUT_POINT_H_
