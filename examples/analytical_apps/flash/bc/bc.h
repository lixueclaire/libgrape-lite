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

#ifndef EXAMPLES_ANALYTICAL_APPS_FLASH_BC_H_
#define EXAMPLES_ANALYTICAL_APPS_FLASH_BC_H_

#include <grape/grape.h>

#include "../api.h"
#include "../flash_app_base.h"
#include "grape/fragment/immutable_edgecut_fragment.h"

namespace grape {
namespace flash {

template <typename FRAG_T, typename VALUE_T>
class BCFlash : public FlashAppBase<FRAG_T, VALUE_T> {
 public:
  using fragment_t = FRAG_T;
  using vid_t = typename fragment_t::vid_t;
  using vertex_t = typename fragment_t::vertex_t;
  using value_t = VALUE_T;
  using adj_list_t = typename fragment_t::adj_list_t;
  using vset_t = VertexSubset<fragment_t, value_t>;
  bool sync_all_ = false;

  float* Res(value_t* v) { return &(v->b); }

  void Run(const fragment_t& graph, vid_t source) {
    Print("Run BC with Flash, source = %d\n", source);
    int n_vertex = graph.GetTotalVerticesNum();
    Print("Total vertices: %d\n", n_vertex);
    vset_t a = All;
    vid_t gid;
    graph.GetVertexMap()->GetGid(source, gid);
    vid_t s = All.fw->Gid2Key(gid);

    int curLevel;

	  DefineMapV(init) {
		  if (id == s) {v.d = 0; v.c = 1; v.b = 0;}
		  else {v.d = -1; v.c = 0; v.b= 0;}
	  };
	  DefineFV(filter) {return id == s;};

	  DefineMapE(update1) { d.c += s.c; };
	  DefineFV(cond) { return v.d == -1; };
	  DefineMapE(reduce1) { d.c += s.c; };
	  DefineMapV(local) { v.d = curLevel; };

	  //DefineFE(check2) { return true; return (d.d == s.d - 1); };
	  DefineMapE(update2) { d.b += d.c / s.c * (1 + s.b); };

	  std::function<void(vset_t&, int)> bn = [&](vset_t &S, int h) {
		  curLevel = h;
		  int sz = VSize(S);
		  if (sz == 0) return; 
      Print("size=%d\n", sz);
		  vset_t T = EdgeMap(S, ED, CTrueE, update1, cond, reduce1);
		  T = VertexMap(T, CTrueV, local);
		  bn(T, h+1);
		  Print("-size=%d\n", sz);
		  curLevel = h;
		  EdgeMap(T, EjoinV(ER, S), CTrueE, update2, CTrueV);
	  };

	  vset_t S = VertexMap(All, CTrueV, init);
	  S = VertexMap(S, filter);

	  bn(S, 1);
  }
};

}  // namespace flash
}  // namespace grape

#endif  // EXAMPLES_ANALYTICAL_APPS_FLASH_BC_H_
