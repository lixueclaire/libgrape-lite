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

#ifndef EXAMPLES_ANALYTICAL_APPS_FLASH_FLUID_COMMUNITY_H_
#define EXAMPLES_ANALYTICAL_APPS_FLASH_FLUID_COMMUNITY_H_

#include <grape/grape.h>

#include "../api.h"
#include "../flash_app_base.h"
#include "grape/fragment/immutable_edgecut_fragment.h"


namespace grape {
namespace flash {

template <typename FRAG_T, typename VALUE_T>
class FluidCommunityFlash : public FlashAppBase<FRAG_T, VALUE_T> {
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

 	int* Res(value_t* v) { return &(v->lab); }

  void Run(const fragment_t& graph) {
    Print("Run fluid-community with Flash, ");
    int64_t n_vertex = graph.GetTotalVerticesNum();
    Print("total vertices: %lld\n", n_vertex);

		int s = 10, iter_max = 100;
		std::vector<int> c(s), cnt(s, 0), cnt_loc(s, 0);
		for	(int i = 0; i < s; ++i)
			c[i] = rand() % n_vertex;
		std::sort(c.begin(), c.end());

		DefineMapV(init) {
			v.lab = locate(c, (int)id);
			if (v.lab == s) v.lab = -1; else ++cnt_loc[v.lab];
			v.l1 = -2; v.l2 = -2;
		};
		VertexMapSeq(All, CTrueV, init);
		DefineFV(filter) { return v.lab >= 0; };

		vset_t A = VertexMap(All, filter);

		std::vector<double> d(s);
		DefineMapV(update) {
			v.old = v.lab;
			if (v.lab >= 0) {
				v.l2 = v.l1; 
				v.l1 = v.lab;
			}
			int pre = v.lab;
			memset(d.data(), 0, sizeof(double) * s);
			if (v.lab >= 0) 
				d[v.lab] = 1.0/cnt[v.lab];
			for_nb(if (nb.lab >= 0) d[nb.lab] += 1.0/cnt[nb.lab];);
			for(int i = 0; i < s; ++i)
				if(d[i] > 1e-10 && (v.lab == -1 || d[i] > d[v.lab] + 1e-10))
					v.lab = i;
			if (v.lab >= 0) ++cnt_loc[v.lab];
			if (pre >= 0) --cnt_loc[pre];
		};

		for(int len = VSize(A), j = 0; len > 0 && j < iter_max; len = VSize(A), ++j) {
			Reduce(cnt_loc, cnt, for_i(cnt[i] += cnt_loc[i]));
			int t_cnt = 0; 
			for(int i = 0; i < s; ++i) 
				t_cnt += cnt[i];
			Print("Round %d: size=%d, t_cnt=%d\n", j, len, t_cnt);
			VertexMapSeq(All, CTrueV, update, false);

			DefineFV(filter1) { return v.lab != v.old; };
			DefineMapV(local) { v.old = v.lab; }; 
			A = VertexMap(All, filter1, local);

			DefineFV(filter2) { return v.lab != v.l2; };
			A = VertexMap(A, filter2);
		}
	}
};

}  // namespace flash
}  // namespace grape

#endif  // EXAMPLES_ANALYTICAL_APPS_FLASH_FLUID_COMMUNITY_H_
