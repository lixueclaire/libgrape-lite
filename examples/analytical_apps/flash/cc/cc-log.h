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

#ifndef EXAMPLES_ANALYTICAL_APPS_FLASH_CC_LOG_H_
#define EXAMPLES_ANALYTICAL_APPS_FLASH_CC_LOG_H_

#include <grape/grape.h>

#include "../api.h"
#include "../flash_app_base.h"
#include "grape/fragment/immutable_edgecut_fragment.h"

#define jump(A) VertexMap(A, checkj, updatej)

#define star(A) S = VertexMap(A, CTrueV, locals); \
S = VertexMap(S, checkj, locals2);\
EdgeMapSparse(S, edges2, CTrueE, updates, CTrueV, updates);\
VertexMap(A, checks, locals2);

#define hook(A) S = VertexMap(A, filterh1); \
VertexMap(S, CTrueV, localh1);\
EdgeMapSparse(S, EU, f2, h2, CTrueV, h2);\
VertexMap(S, filterh2, localh2);


namespace grape {
namespace flash {

template <typename FRAG_T, typename VALUE_T>
class CCLogFlash : public FlashAppBase<FRAG_T, VALUE_T> {
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

  int* Res(value_t* v) { return &(v->p); }

  void Run(const fragment_t& graph) {
    int n_vertex = graph.GetTotalVerticesNum();
    Print("Run cc-log with Flash, total vertices: %d\n", n_vertex);
		vset_t S;
		bool c = true;
	
		DefineMapV(init) { v.p = id; v.s = false; v.f = id; };
		VertexMap(All, CTrueV, init);

		DefineFE(check1) { return sid < d.p; };
		DefineMapE(update1) { d.p = std::min(d.p, (int)sid); };
		vset_t A = EdgeMapDense(All, EU, check1, update1, CTrueV);

		DefineOutEdges(edges) { VjoinP(p) };
		DefineMapE(update2) { d.s = true; };
		EdgeMapSparse(A, edges, CTrueE, update2, CTrueV, update2);

		DefineFV(filter1) { return v.p == id && (!v.s); };
		DefineMapV(local1) { v.p = INT_MAX; };
		A = VertexMap(All, filter1, local1);
		EdgeMapDense(All, EjoinV(EU, A), check1, update1, CTrueV);

		DefineFV(filter2) { return v.p != INT_MAX; };
		A = VertexMap(All, filter2);

		DefineFV(checkj) { return GetV(v.p)->p != v.p; };
		DefineMapV(updatej) { v.p = GetV(v.p)->p; };

		DefineOutEdges(edges2) {
			std::vector<vid_t> res; 
			res.clear(); 
			res.push_back(GetV(v.p)->p); 
			return res;
		};
		DefineMapV(locals) { v.s = true; };
		DefineMapV(locals2) { v.s = false; };
		DefineMapE(updates) { d.s = false; };
		DefineFV(checks) { return (v.s && !(GetV(v.p)->s)); };

		DefineFV(filterh1) { return v.s; };
		DefineMapV(localh1) {
			v.f = c ? v.p : INT_MAX; 
			for_nb( if (nb.p != v.p) v.f = std::min(v.f, nb.p); )
		};
		DefineFE(f2) { return s.p != sid && s.f != INT_MAX && s.f != s.p && s.p == did; };
		DefineMapE(h2) { d.f = std::min(d.f, s.f); };

		DefineFV(filterh2) { return v.p == id && v.f != INT_MAX && v.f != v.p; };
		DefineMapV(localh2) { v.p = v.f; };

		for (int i = 0, len = 0; VSize(A) > 0; ++i) {
			len = VSize(jump(A));
			if (len == 0) break;
			Print("Round %d,len=%d\n", i, len);
			jump(A); jump(A);
			star(A); c = true; hook(A);
			star(A); c = false; hook(A);
		}

		DefineFV(filter3) { return v.p == INT_MAX; };
		DefineMapV(local3) { v.p = id; };
		VertexMap(All, filter3, local3);
  }
};

}  // namespace flash
}  // namespace grape

#endif  // EXAMPLES_ANALYTICAL_APPS_FLASH_CC_LOG_H_
