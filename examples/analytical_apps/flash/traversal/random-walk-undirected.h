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

#ifndef EXAMPLES_ANALYTICAL_APPS_FLASH_RANDOM_WALK_UNDIRECTED_H_
#define EXAMPLES_ANALYTICAL_APPS_FLASH_RANDOM_WALK_UNDIRECTED_H_

#include <grape/grape.h>

#include "../api.h"
#include "../flash_app_base.h"
#include "grape/fragment/immutable_edgecut_fragment.h"

namespace grape {
namespace flash {

template <typename FRAG_T, typename VALUE_T>
class RandomWalkUndirectedFlash : public FlashAppBase<FRAG_T, VALUE_T> {
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
  int invalid = -1;

  int* Res(value_t* v) { if (v->walk.size() == 0) return &invalid; else return &(v->walk.back()); }

  void Run(const fragment_t& graph) {
    Print("Run Random Walk with Flash, ");
    int n_vertex = graph.GetTotalVerticesNum();
    Print("total vertices: %d\n", n_vertex);

    DefineFV(filter) { return Deg(id) > 0; };
    DefineMapV(init_v) {
      vertex_t u;
      u.SetValue(All.fw->Key2Lid(id));
      auto oes = graph.GetOutgoingAdjList(u);
      auto ies = graph.GetIncomingAdjList(u);
      int i = rand() % (oes.Size() + ies.Size());
      vid_t nb_id;
      if (i < oes.Size())
        nb_id = graph.Vertex2Gid((oes.begin() + i)->get_neighbor());
      else {
        i -= oes.Size();
        nb_id = graph.Vertex2Gid((ies.begin() + i)->get_neighbor());
      }
      nb_id = All.fw->Gid2Key(nb_id);
      v.walk.push_back(nb_id);
    };
    vset_t A = VertexMap(All, filter, init_v);

    DefineMapV(local) { v.from.clear(); v.from2.clear(); };
    DefineOutEdges(edges1) {
		  std::vector<vid_t> res; 
		  res.clear(); 
		  res.push_back(v.walk.back()); 
		  return res;
	  };
    DefineMapE(update1) { d.from.push_back(sid); };
    DefineMapE(reduce1) { 
      for (auto &i: s.from)
        d.from.push_back(i);
    };

	  DefineOutEdges(edges2) {
      std::vector<vid_t> res; 
		  res.clear(); 
      for (auto &i: v.from)
		    res.push_back(i); 
      return res;
    };
    DefineFE(check2) { return Deg(sid) > 0; };
	  DefineMapE(update2) { 
      vertex_t u;
      u.SetValue(All.fw->Key2Lid(sid));
      auto oes = graph.GetOutgoingAdjList(u);
      auto ies = graph.GetIncomingAdjList(u);
      int i = rand() % (oes.Size() + ies.Size());
      vid_t nb_id;
      if (i < oes.Size())
        nb_id = graph.Vertex2Gid((oes.begin() + i)->get_neighbor());
      else {
        i -= oes.Size();
        nb_id = graph.Vertex2Gid((ies.begin() + i)->get_neighbor());
      }
      nb_id = All.fw->Gid2Key(nb_id);
      d.from2.push_back(nb_id); 
    };
	  DefineMapE(reduce2) { d.from2 = s.from2; };

    int step = 5;
    for (int i = 1; i < step; i++) {
      Print("Walk Step = %d\n", i);
      vset_t B = VertexMap(A, CTrueV, local);
	  	B = EdgeMapSparse(B, edges1, CTrueE, update1, CTrueV, reduce1);
		  B = EdgeMapSparse(B, edges2, check2, update2, CTrueV, reduce2);

      DefineMapV(update) { v.walk.push_back(v.from2.front()); };
      B = VertexMap(B, CTrueV, update);
    }
  }
};

}  // namespace flash
}  // namespace grape

#endif  // EXAMPLES_ANALYTICAL_APPS_FLASH_RANDOM_WALK_UNDIRECTED_H_
