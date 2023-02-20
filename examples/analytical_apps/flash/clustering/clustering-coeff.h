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

#ifndef EXAMPLES_ANALYTICAL_APPS_FLASH_CLUSTERING_COEFF_H_
#define EXAMPLES_ANALYTICAL_APPS_FLASH_CLUSTERING_COEFF_H_

#include <grape/grape.h>

#include "../api.h"
#include "../flash_app_base.h"
#include "grape/fragment/immutable_edgecut_fragment.h"

#define COND nb.deg>v.deg || (nb.deg==v.deg && nb_id > id)

namespace grape {
namespace flash {

template <typename FRAG_T, typename VALUE_T>
class ClusteringCoeffFlash : public FlashAppBase<FRAG_T, VALUE_T> {
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

 	int32_t* Res(value_t* v) { return &(v->count); }

  void Run(const fragment_t& graph) {
    Print("Run clustering-coeff with Flash, ");
    int64_t n_vertex = graph.GetTotalVerticesNum();
    Print("total vertices: %lld\n", n_vertex);

		DefineMapV(init) {v.count = 0; v.deg = Deg(id); };
		VertexMap(All, CTrueV, init);

		Print("Loading...\n");
		DefineMapV(local) {
			v.out.clear();
			for_nb( if (COND) v.out.push_back(nb_id); );
		};
		VertexMap(All, CTrueV, local);

		Print("Computing...\n");

		std::vector<bool> p(n_vertex, false);
		std::vector<long long> cnt(n_vertex, 0);

		DefineMapV(update) {
			for (auto u: v.out) p[u] = true;
			for_nb(
				if (COND)
					for (auto &u: nb.out) 
						if (p[u]) { cnt[id]++; cnt[nb_id]++; cnt[u]++; }
			);	
			for (auto u: v.out) p[u] = false;
		};


		VertexMapSeq(All, CTrueV, update);

	  long long cnt_loc = 0, cnt_all;
		Traverse( cnt_loc += cnt[id]; );
		GetSum(cnt_loc, cnt_all);
		Print("Totol count = %lld\n", cnt_all);
	}
};

}  // namespace flash
}  // namespace grape

#endif  // EXAMPLES_ANALYTICAL_APPS_FLASH_CLUSTERING_COEFF_H_
