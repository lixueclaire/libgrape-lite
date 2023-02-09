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

#ifndef EXAMPLES_ANALYTICAL_APPS_FLASH_CC_OPT_H_
#define EXAMPLES_ANALYTICAL_APPS_FLASH_CC_OPT_H_

#include <grape/grape.h>

#include "../api.h"
#include "../flash_app_base.h"
#include "grape/fragment/immutable_edgecut_fragment.h"

namespace grape {
namespace flash {

template <typename FRAG_T, typename VALUE_T>
class CCOptFlash : public FlashAppBase<FRAG_T, VALUE_T> {
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

  long long* Res(value_t* v) { return &(v->cid); }

  void Run(const fragment_t& graph) {
    int n_vertex = graph.GetTotalVerticesNum();
    Print("Run CC with Flash, total vertices: %d\n", n_vertex);
   	long long v_loc = 0, v_glb = 0;
	
	  DefineMapV(init) {
		  v.cid = Deg(id) * (long long) n_vertex + id; 
		  v_loc = std::max(v_loc, v.cid);
	  };
	  DefineFV(filter) { return v.cid == v_glb; };

	  VertexMap(All, CTrueV, init);
    GetMax(v_loc, v_glb);;
	  vset_t A = VertexMap(All, filter);

	  DefineFV(cond) { return v.cid != v_glb; };
	  DefineMapE(update) { d.cid = v_glb; };
	  DefineMapE(reduce) { d = s; };

	  for(int len = VSize(A), i = 0; len > 0; len = VSize(A), ++i) {
		  Print("Round 0.%d: size=%d\n", i, len);
		  A = EdgeMap(A, EU, CTrueE, update, cond, reduce);
	  }

	  DefineFV(filter2) { return v.cid != v_glb; };
	  A = VertexMap(All, filter2);

	  DefineFE(check2) { return s.cid > d.cid; };
	  DefineMapE(update2) { d.cid = std::max(d.cid, s.cid); };

	  for(int len = VSize(A), i = 0; len > 0; len = VSize(A),++i) {
		  Print("Round 1.%d: size=%d\n", i, len);
		  A = EdgeMap(A, EU, check2, update2, CTrueV, update2);
	  }
  }
};

}  // namespace flash
}  // namespace grape

#endif  // EXAMPLES_ANALYTICAL_APPS_FLASH_CC_OPT_H_
