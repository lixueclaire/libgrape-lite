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

#ifndef EXAMPLES_ANALYTICAL_APPS_FLASH_DFS_H_
#define EXAMPLES_ANALYTICAL_APPS_FLASH_DFS_H_

#include <grape/grape.h>

#include "../api.h"
#include "../flash_app_base.h"
#include "grape/fragment/immutable_edgecut_fragment.h"

namespace grape {
namespace flash {

template <typename FRAG_T, typename VALUE_T>
class DFSFlash : public FlashAppBase<FRAG_T, VALUE_T> {
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

  int* Res(value_t* v) { return &(v->f); }

  void Run(const fragment_t& graph) {
    Print("Run DFS with Flash, ");
    int n_vertex = graph.GetTotalVerticesNum();
    Print("total vertices: %d\n", n_vertex);

    std::vector<int> x(n_vertex), y(n_vertex), d(n_vertex), deg(n_vertex);
	  std::vector<std::vector<int>> sub(n_vertex);
	  int p = 0;

	  std::function<void(int,int)> dfs=[&](int u, int nowd) {
		  x[u] = p++;
		  d[u] = nowd;
		  for(auto &v : sub[u]) dfs(v, nowd+1);
		  y[u] = p++;
	  };

    DefineMapV(init) { v.f = -1; v.deg = Deg(id); };

	  vset_t A = VertexMap(All, CTrueV, init);

	  Traverse(deg[id] = v.deg;);
	  for(int len = n_vertex, i = 0; len > 0; len = VSize(A), ++i) {
		  p = 0;
		  Traverse(if (v.f == -1) dfs(id, 0););
      DefineMapV(local) {
        v.pre = v.f;
        for_in(if(y[nb_id] < x[id] && (v.f == v.pre || deg[nb_id]< deg[v.f])) v.f = nb_id; );
      };
      VertexMapSeq(All, CTrueV, local, false);
      DefineFV(filter) { return v.pre != v.f; };
      DefineMapV(update) { v.pre = v.f; };
      A = VertexMap(All, filter, update);
		  Traverse(if (v.f != -1 && y[v.f] < x[id]) sub[v.f].push_back(id););
		  Traverse(
        int s = 0;
			  for(auto u : sub[id])
				if(GetV(u)->f == id)
					sub[id][s++] = u;
			  sub[id].resize(s);
		  );
		  int maxd = 0; 
      for(auto nowd:d) maxd = std::max(maxd, nowd);
		  Print("Round %d: len=%d, maxd=%d\n", i, len, maxd);
	  }
  }
};

}  // namespace flash
}  // namespace grape

#endif  // EXAMPLES_ANALYTICAL_APPS_FLASH_DFS_H_
