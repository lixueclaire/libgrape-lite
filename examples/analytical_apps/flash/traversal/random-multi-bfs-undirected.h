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

#ifndef EXAMPLES_ANALYTICAL_APPS_FLASH_RANDOM_MULTI_BFS_UNDIRECTED_H_
#define EXAMPLES_ANALYTICAL_APPS_FLASH_RANDOM_MULTI_BFS_UNDIRECTED_H_

#include <grape/grape.h>

#include "../api.h"
#include "../flash_app_base.h"
#include "grape/fragment/immutable_edgecut_fragment.h"

namespace grape {
namespace flash {

template <typename FRAG_T, typename VALUE_T>
class RandomMultiBFSUndirectedFlash : public FlashAppBase<FRAG_T, VALUE_T> {
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

  int* Res(value_t* v) { return &(v->res); }

  void Run(const fragment_t& graph) {
    Print("Run undirected random multi-source BFS with Flash, ");
    int n_vertex = graph.GetTotalVerticesNum();
    Print("total vertices: %d\n", n_vertex);

    std::vector<vid_t> s; 
    long long one = 1; int k = 64;
	  for(int i = 0; i < k; ++i) s.push_back(rand() % n_vertex);

    DefineMapV(init) { 
      v.d.resize(k);
      for (int i = 0; i < k; i++) v.d[i] = -1;
      v.seen = 0;
    };
	  vset_t C = VertexMap(All, CTrueV, init);

    DefineFV(filter) { return find(s, id); };
    DefineMapV(local) { 
      int p = locate(s, id);
      v.seen |= one<<p;
      v.d[p] = 0;
    };
    vset_t S = VertexMap(All, filter, local);

    for(int len = VSize(S), i = 1; len > 0; len = VSize(S), ++i) {
		  Print("Round %d: size=%d\n", i, len);
      DefineFE(check) { return (s.seen & (~d.seen)); };
      DefineMapE(update) { 
        int64_t b = (s.seen & (~d.seen));
        if (b) d.seen |= b; 
        for (int p = 0; p < k; ++p)  
          if (b & (one<<p)) 
            d.d[p] = i;
      };
      S = EdgeMapDense(All, EU, check, update, CTrueV);
    }

    DefineMapV(final) { v.res = -1; for (int i = 0; i < k; ++i) v.res = std::max(v.res, v.d[i]); };
    VertexMap(All, CTrueV, final);
  }
};

}  // namespace flash
}  // namespace grape

#endif  // EXAMPLES_ANALYTICAL_APPS_FLASH_RANDOM_MULTI_BFS_UNDIRECTED_H_
