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

#ifndef EXAMPLES_ANALYTICAL_APPS_FLASH_FLUID_BY_COLOR_H_
#define EXAMPLES_ANALYTICAL_APPS_FLASH_FLUID_BY_COLOR_H_

#include <grape/grape.h>

#include "../api.h"
#include "../flash_app_base.h"
#include "grape/fragment/immutable_edgecut_fragment.h"


namespace grape {
namespace flash {

template <typename FRAG_T, typename VALUE_T>
class FluidByColorFlash : public FlashAppBase<FRAG_T, VALUE_T> {
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

 	int* Res(value_t* v) { return &(v->label); }

  void Run(const fragment_t& graph) {
    Print("Run fluid-community by coloring with Flash, ");
    int64_t n_vertex = graph.GetTotalVerticesNum();
    Print("total vertices: %lld\n", n_vertex);

		DefineMapV(init) { v.c = 0; v.deg = Deg(id); v.colors.clear(); };
		vset_t A = VertexMap(All, CTrueV, init);

		DefineFE(check) { return (s.deg > d.deg) || (s.deg == d.deg && sid > did); };
		DefineMapE(update) { d.colors.push_back(s.c); };
	
		DefineMapV(local1) {
			std::set<int> used;
			used.clear();
			for (auto &i : v.colors) 
				used.insert(i);
			for (int i = 0; ; ++i) 
				if (used.find(i) == used.end()) { v.cc = i; break; }
			v.colors.clear();
		};

		DefineFV(filter) { return v.cc != v.c; };
		DefineMapV(local2) { v.c = v.cc; };

		for(int len = VSize(A), i = 0; len > 0; len = VSize(A), ++i) {
			Print("Color Round %d: size=%d\n", i, len);
			A = EdgeMapDense(All, EU, check, update, CTrueV, false);
			A = VertexMap(All, CTrueV, local1, false);
			A = VertexMap(All, filter, local2);
		}

		int loc_max_color = 0, max_color; 
		for(auto &id: All.s) 
			loc_max_color = std::max(loc_max_color, (int)GetV(id)->c);
		GetMax(loc_max_color, max_color);
		max_color += 1;
		Print("max_color=%d\n", max_color);

		std::vector<vset_t> cset(max_color);
		A = All;
		for (short i = 0; i < max_color; ++i) {
			DefineFV(filter1) { return v.c == i; };
			DefineFV(filter2) { return v.c > i; };
			cset[i] = VertexMap(A, filter1);
			A = VertexMap(A, filter2);
		}

		int s = 10;
		std::vector<int> c(s), cnt(s, 0), cnt_loc(s, 0);
		for	(int i = 0; i < s; ++i)
			c[i] = rand() % n_vertex;
		std::sort(c.begin(), c.end());

		DefineMapV(init_l) {
			v.t = -max_color;
			v.label = locate(c, (int)id);
			if (v.label == s) v.label = -1; else ++cnt_loc[v.label];
		};
		VertexMapSeq(All, CTrueV, init_l);
		DefineFV(filter_l) { return v.label >= 0; };
		DefineMapV(local_l) { v.t = 0; };
		DefineMapE(push) { d.t = 0; };

		A = VertexMap(All, filter_l, local_l);
		EdgeMapSparse(A, EU, CTrueE, push, CTrueV, push);

		std::vector<double> d(s);
		DefineMapV(relabel) {
			int pre = v.label;
			v.old = v.label;
			memset(d.data(), 0, sizeof(double) * s);
			if (v.label >= 0) d[v.label] = 1.0 / cnt[v.label];
			for_nb (if(nb.label >= 0) d[nb.label] += 1.0/cnt[nb.label]; );
			for(int i = 0; i < s; ++i)
				if (d[i] > 1e-10 && (v.label == -1 || d[i] > d[v.label] + 1e-10))
					v.label = i;
			if (v.label >= 0) ++cnt_loc[v.label];
			if (pre >= 0) --cnt_loc[pre];
		};

		std::vector<int> t_loc(max_color, 0), t_glb(max_color, 0);
		for(int len = VSize(A), j = 0, nowt = 0; len > 0; ++j) {
			Reduce(cnt_loc, cnt, for_i(cnt[i] += cnt_loc[i]));
			int t_cnt = 0; 
			for(int i = 0; i < s; ++i) t_cnt += cnt[i];
			Print("Label Round %d: size=%d,t_cnt=%d\n", j, len, t_cnt);
			len = 0;
			for(int j = 0; j < max_color; ++j) {
				++nowt;
				if (t_glb[j] < nowt - max_color) continue;

				DefineFV(filter3) { return v.t >= nowt - max_color; };
				A = VertexMap(cset[j], filter3);
				A = VertexMapSeq(A, CTrueV, relabel, false);
				DefineFV(check) { return v.old != v.label; };
				DefineMapV(update) { v.old = v.label; };
				A = VertexMap(A, check, update);

				len += VSize(A);
				DefineMapE(updated) { d.t = nowt; };
				vset_t B = EdgeMapSparse(A, EU, CTrueE, updated, CTrueV, updated);
				DefineMapV(localv) { t_loc[v.c] = nowt; };
				VertexMapSeq(B, CTrueV, localv);
				Reduce(t_loc, t_glb, for_k(t_glb[k] = std::max(t_glb[k], t_loc[k])));
			}
		}
	}
};

}  // namespace flash
}  // namespace grape

#endif  // EXAMPLES_ANALYTICAL_APPS_FLASH_FLUID_BY_COLOR_H_
