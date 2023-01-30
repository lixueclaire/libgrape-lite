/** Copyright 2022 Alibaba Group Holding Limited.

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

#ifndef EXAMPLES_ANALYTICAL_APPS_FLASH_APP_API_H_
#define EXAMPLES_ANALYTICAL_APPS_FLASH_APP_API_H_

#include "flash/func.h"
#include "flash/vertex_subset.h"

namespace grape {
namespace flash {

template <typename fragment_t, typename value_t, class F>
VSet vertexMapFunction(const fragment_t& graph, VSet& U, F& f) {
  VSet res;
  /*U.fw->ForEach(U.s.begin(), U.s.end(),
                [&U, &f](int tid, typename fragment_t::vid_t key) {
                  if (f(key, *(U.fw->Get(key))))
                    U.fw->SetActive(key);
                });

  U.fw->GetActiveVertices(res.s);*/
  for (auto &key : U.s) {
    if (f(key, *(U.fw->Get(key))))
      res.s.push_back(key);
  }
  return res;
}

template <typename fragment_t, typename value_t, class F, class M>
VSet vertexMapFunction(const fragment_t& graph, VSet& U, F& f, M& m) {
  VSet res;
  U.fw->ForEach(U.s.begin(), U.s.end(),
                [&U, &f, &m](int tid, typename fragment_t::vid_t key) {
                  value_t v = *(U.fw->Get(key));
                  if (!f(key, v))
                    return;
                  m(key, v);
                  U.fw->PutNext(key, v, false, tid);
                });
  U.fw->Barrier();
  U.fw->GetActiveVerticesAndSetStates(res.s);
  return res;
}

template <typename fragment_t, typename value_t, class F, class M, class C>
VSet edgeMapDenseFunction(const fragment_t& graph, VSet& U, int h, F& f, M& m,
                          C& c, bool b = true) {
  using vertex_t = typename fragment_t::vertex_t;
  using vid_t = typename fragment_t::vid_t;
  bool flag = ((&U) == (&All));
  if (!flag)
    U.ToDense();
  VSet res;

  U.fw->ForEach(graph.InnerVertices(),
                [&flag, &graph, &U, &h, &f, &m, &c](int tid, vertex_t u) {
                  vid_t vid = graph.GetId(u);
                  value_t v = *(U.fw->Get(vid));
                  if (!c(vid, v))
                    return;
                  if (h == EU || h == ED) {
                    auto es = graph.GetIncomingAdjList(u);
                    for (auto& e : es) {
                      vid_t nb_id = graph.GetId(e.get_neighbor());
                      if (flag || U.IsIn(nb_id)) {
                        value_t nb = *(U.fw->Get(nb_id));
                        if (f(nb_id, vid, nb, v)) {
                          m(nb_id, vid, nb, v);
                          U.fw->SetDirty(vid);
                          if (!c(vid, v))
                            break;
                        }
                      }
                    }
                  }
                  if (h == EU || h == ER) {
                    auto es = graph.GetOutgoingAdjList(u);
                    for (auto& e : es) {
                      vid_t nb_id = graph.GetId(e.get_neighbor());
                      if (flag || U.IsIn(nb_id)) {
                        value_t nb = *(U.fw->Get(nb_id));
                        if (f(nb_id, vid, nb, v)) {
                          m(nb_id, vid, nb, v);
                          U.fw->SetDirty(vid);
                          if (!c(vid, v))
                            break;
                        }
                      }
                    }
                  }
                  if (U.fw->IsDirty(vid))
                    U.fw->PutNext(vid, v, false, tid);
                });

  U.fw->Barrier();
  U.fw->GetActiveVertices(res.s, res.d);
  res.is_dense = true;
  return res;
}

template <typename fragment_t, typename value_t, class F, class M, class C>
VSet doEdgeMapSparse(const fragment_t& graph, VSet& U, int h, F& f, M& m,
                     C& c) {
  using vertex_t = typename fragment_t::vertex_t;
  using vid_t = typename fragment_t::vid_t;
  VSet res;

  for (auto& vid : U.s) {
    vertex_t u;
    if (!graph.GetVertex(vid, u))
      continue;
    value_t v = *(U.fw->Get(vid));
    if (h == EU || h == ED) {
      auto es = graph.GetOutgoingAdjList(u);
      for (auto& e : es) {
        vid_t nb_id = graph.GetId(e.get_neighbor());
        value_t nb = *(U.fw->Get(nb_id));
        if (c(nb_id, nb) && f(vid, nb_id, v, nb)) {
          m(vid, nb_id, v, nb);
          U.fw->PutNext(nb_id, nb, true);
        }
      }
    }
    if (h == EU || h == ER) {
      auto es = graph.GetIncomingAdjList(u);
      for (auto& e : es) {
        vid_t nb_id = graph.GetId(e.get_neighbor());
        value_t nb = *(U.fw->Get(nb_id));
        if (c(nb_id, nb) && f(vid, nb_id, v, nb)) {
          m(vid, nb_id, v, nb);
          U.fw->PutNext(nb_id, nb, true);
        }
      }
    }
  }

  /*U.fw->ForEach(U.s.begin(), U.s.end(), [&graph, &U, &h, &f, &m, &c](int tid, vid_t vid) { 
    vertex_t u; 
    if (!graph.GetVertex(vid, u)) return; 
    value_t v = *(U.fw->Get(vid)); 
    if (h == EU || h == ED) { 
      auto es = graph.GetOutgoingAdjList(u); 
      for (auto &e: es) { 
        vid_t nb_id = graph.GetId(e.get_neighbor()); 
        value_t nb = *(U.fw->Get(nb_id)); 
        if (c(nb_id, nb) && f(vid, nb_id, v, nb)) {
           m(vid, nb_id, v, nb); 
           U.fw->PutNext(nb_id, nb, true, tid);
        }
      }
    }
    if (h == EU || h == ER) {
      auto es = graph.GetIncomingAdjList(u);
      for (auto &e: es) {
        vid_t nb_id = graph.GetId(e.get_neighbor());
        value_t nb = *(U.fw->Get(nb_id));
        if (c(nb_id, nb) && f(vid, nb_id, v, nb)) {
          m(vid, nb_id, v, nb);
          U.fw->PutNext(nb_id, nb, true, tid);
        }
      }
    }
  });*/

  U.fw->Barrier(true);
  U.fw->GetActiveVerticesAndSetStates(res.s);
  return res;
}

template <typename fragment_t, typename value_t, class F, class M, class C>
inline VSet edgeMapSparseFunction(const fragment_t& graph, VSet& U, int h, F& f,
                                  M& m, C& c) {
  U.fw->ResetAggFunc();
  return doEdgeMapSparse(graph, U, h, f, m, c);
}

template <typename fragment_t, typename value_t, class F, class M, class C,
          class R>
inline VSet edgeMapSparseFunction(const fragment_t& graph, VSet& U, int h, F& f,
                                  M& m, C& c, const R& r) {
  U.fw->SetAggFunc(r);
  return doEdgeMapSparse(graph, U, h, f, m, c);
}

template <typename fragment_t, typename value_t, class F, class M, class C>
inline VSet edgeMapFunction(const fragment_t& graph, VSet& U, int h, F& f, M& m,
                            C& c) {
  int len = VSize(U);
  if (len > THRESHOLD)
    return edgeMapDenseFunction(graph, U, h, f, m, c);
  else
    return edgeMapSparseFunction(graph, U, h, f, m, c);
}

template <typename fragment_t, typename value_t, class F, class M, class C,
          class R>
inline VSet edgeMapFunction(const fragment_t& graph, VSet& U, int h, F& f, M& m,
                            C& c, const R& r) {
  int len = VSize(U);
  if (len > THRESHOLD)
    return edgeMapDenseFunction(graph, U, h, f, m, c);
  else
    return edgeMapSparseFunction(graph, U, h, f, m, c, r);
}

}  // namespace flash
}  // namespace grape

#endif  // EXAMPLES_ANALYTICAL_APPS_FLASH_APP_API_H_
