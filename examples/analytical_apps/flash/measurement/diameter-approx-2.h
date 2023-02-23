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

#ifndef EXAMPLES_ANALYTICAL_APPS_FLASH_DIAMETER_APPROX_2_H_
#define EXAMPLES_ANALYTICAL_APPS_FLASH_DIAMETER_APPROX_2_H_

#include <grape/grape.h>

#include "../api.h"
#include "../flash_app_base.h"
#include "grape/fragment/immutable_edgecut_fragment.h"

namespace grape {
namespace flash {

template <typename FRAG_T, typename VALUE_T>
class DiameterApprox2Flash : public FlashAppBase<FRAG_T, VALUE_T> {
 public:
  using fragment_t = FRAG_T;
  using vid_t = typename fragment_t::vid_t;
  using vertex_t = typename fragment_t::vertex_t;
  using value_t = VALUE_T;
  using vdata_t = typename fragment_t::vdata_t;
  using edata_t = typename fragment_t::edata_t;
  using adj_list_t = typename fragment_t::adj_list_t;
  using vset_t = VertexSubset<fragment_t, value_t>;
  using E = std::pair<edata_t, std::pair<vid_t, vid_t> >;
  bool sync_all_ = false;

  int* Res(value_t* v) { return &(v->ecc); }

  void Run(const fragment_t& graph) {
    Print("Run diameter-approx with Flash, ");
    int n_vertex = graph.GetTotalVerticesNum();
    Print("total vertices: %d\n", n_vertex);

    int64_t v_loc = 0, v_glb = 0;
    DefineMapV(init) {
      int64_t v_now = Deg(id) * (int64_t)n_vertex + id; 
      v_loc = std::max(v_loc, v_now);
      v.dis = -1;
      v.ecc = 0;
    };
    VertexMapSeq(All, CTrueV, init);
	  GetMax(v_loc, v_glb);

    DefineFV(filter) { return id == v_glb % n_vertex; };
    DefineMapV(local) { v.dis = 0; };
    vset_t A = VertexMap(All, filter, local);

    int dt = 0;
	  for(int len = VSize(A), i = 1; len > 0; len = VSize(A), dt = i++) {
		  Print("Round %d: size=%d\n", i, len);
      DefineFV(cond) { return v.dis == -1; };
      DefineFE(check) { return s.dis != -1; };
      DefineMapE(update) { d.dis = i; };
      A = EdgeMap(A, EU, check, update, cond, update);
	  }

    DefineFV(filter2) { return v.dis != -1; };
    DefineMapV(local2) { v.ecc = std::max(v.dis, dt - v.dis); };
    A = VertexMap(All, filter2, local2);

	  std::vector<vid_t> s; long long one = 1; int k = 64;
	  std::vector<std::pair<int,int>> c(k, std::make_pair(-1, -1)), t(k);
    DefineMapV(cal_c) {
      int p = 0;
      for (int i = 1; i < k; ++i) 
        if (c[i] < c[p]) 
          p = i; 
      if (v.dis > c[p].first)
        c[p] = std::make_pair(v.dis, (int)id);
    };
    VertexMapSeq(A, CTrueV, cal_c);
	  std::sort(c.begin(), c.end());
	  Reduce(c, t, std::reverse(t, t+len); for_i(t[i] = std::max(t[i], c[i])); std::sort(t, t+len));
	  s.clear(); 
    for(int i = 0; i < k; ++i) s.push_back(t[i].second);

    DefineMapV(local3) { v.seen = 0; };
    vset_t S = VertexMap(A, CTrueV, local3);
    DefineFV(filter4) { return find(s, id); };
    DefineMapV(local4) { int p = locate(s, id); v.seen |= one<<p; };
    S = VertexMapSeq(S, filter4, local4);

	  for(int len = VSize(S), i = 1; len > 0; len = VSize(S), ++i) {
		  Print("Round %d: size=%d\n", i, len);
      DefineFE(check) { return (s.seen & (~d.seen)); };
      DefineMapE(update) { 
        d.seen |= (s.seen & (~d.seen)); 
        d.ecc = std::max(d.ecc, std::max(i, dt - i)); 
      };
      S = EdgeMapDense(S, EjoinV(EU, A), check, update, CTrueV);
	  }

	  int d = 0, r = n_vertex, dd, rr;
    for (auto &i: All.s) {
      int e = GetV(i)->ecc;
      d = std::max(d, e);
      if (e != 0) r = std::min(r, e);
    }
    GetMax(d, dd);
    GetMin(r, rr);
	  Print("diameter=%d, radius=%d\n", dd, rr);
  }
};

}  // namespace flash
}  // namespace grape

#endif  // EXAMPLES_ANALYTICAL_APPS_FLASH_DIAMETER_APPROX_2_H_
