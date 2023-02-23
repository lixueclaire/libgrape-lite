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

#ifndef EXAMPLES_ANALYTICAL_APPS_FLASH_SSSP_DLT_STEP_H_
#define EXAMPLES_ANALYTICAL_APPS_FLASH_SSSP_DLT_STEP_H_

#include <grape/grape.h>

#include "../api.h"
#include "../flash_app_base.h"
#include "grape/fragment/immutable_edgecut_fragment.h"

namespace grape {
namespace flash {

template <typename FRAG_T, typename VALUE_T>
class SSSPDltStepFlash : public FlashAppBase<FRAG_T, VALUE_T> {
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

  float* Res(value_t* v) { return &(v->dis); }

  void Run(const fragment_t& graph, vid_t source) {
    Print("Run delta-stepping SSSP with Flash, source = %d\n", source);
    int n_vertex = graph.GetTotalVerticesNum();
    Print("Total vertices: %d\n", n_vertex);
    vid_t gid;
    graph.GetVertexMap()->GetGid(source, gid);
    source = All.fw->Gid2Key(gid);

    float dlt =0, m = 0, dlt_tot, m_tot;
    DefineMapV(init_v) { 
      v.dis = (id == source) ? 0 : -1; 
      for_nb( dlt += weight; m += 1; );
    };
    vset_t a = VertexMapSeq(All, CTrueV, init_v);
    GetSum(dlt, dlt_tot);
    GetSum(m, m_tot);
    dlt = dlt_tot * 2 / m_tot;
    Print("dlt = %0.3f\n", dlt);

    vset_t B = All;
    for(float a = 0, b = dlt, maxd = -1; a < maxd || maxd < 0; a += dlt, b += dlt) {
      Print("a=%0.3f, b=%0.3f\n", a, b);
      DefineFV(filter1) { return v.dis >= a-(1e-10) || v.dis < -0.5; };
      DefineFV(filter2) { return v.dis >= a-(1e-10) && v.dis < b; };
      B = VertexMap(B, filter1);
      vset_t A = VertexMap(B, filter2);

		  for(int len = VSize(A), i = 0; len > 0; len = VSize(A), ++i) {
			  Print("Round %d: size=%d\n", i, len);
        DefineFE(check) { return s.dis >= a-(1e-10) && (d.dis < -0.5 || s.dis + weight < d.dis); };
        DefineMapE(update) { if (d.dis < -0.5 || s.dis + weight < d.dis) d.dis = s.dis + weight; };
        A = EdgeMapDense(A, EjoinV(ED, B), check, update, CTrueV);
        A = VertexMap(A, filter2);
		  }

		  maxd = 0;  float maxd_glb = 0;
      DefineMapV(find_max) { maxd = std::max(maxd, v.dis); };
      VertexMapSeq(All, CTrueV, find_max);
      GetMax(maxd, maxd_glb);
      maxd = maxd_glb;
      Print("maxd=%0.3f\n", maxd);
	  }
  }
};

}  // namespace flash
}  // namespace grape

#endif  // EXAMPLES_ANALYTICAL_APPS_FLASH_SSSP_DLT_STEP_H_
