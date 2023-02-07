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
  for (auto &key : U.s) {
    if (f(key, *(U.fw->Get(key))))
      res.s.push_back(key);
  }
  return res;
}

template <typename fragment_t, typename value_t, class F, class M>
VSet vertexMapFunction(const fragment_t& graph, VSet& U, F& f, M& m) {
  U.fw->ForEach(U.s.begin(), U.s.end(),
                [&U, &f, &m](int tid, typename fragment_t::vid_t key) {
                  value_t v = *(U.fw->Get(key));
                  if (!f(key, v))
                    return;
                  m(key, v);
                  U.fw->PutNextLocal(key, v, tid);
                });
  VSet res;
  U.fw->Barrier();
  U.fw->GetActiveVertices(res.s);
  //U.fw->GetActiveVerticesAndSetStates(res.s);
  /*for (auto &key : U.s) {
    if (f(key, *(U.fw->Get(key)))) {
      m(key, U.fw->states_[key]);
      U.fw->next_states_[key] = U.fw->states_[key];
      U.fw->Synchronize(key);
      res.s.push_back(key);
    }
  }
  U.fw->Barrier();*/
  return res;
}

template <typename fragment_t, typename value_t, class F, class M, class C>
VSet edgeMapDenseFunction(const fragment_t& graph, VSet& U, int h, VSet& T,
                          F& f, M& m, C& c, bool b = true) {
  if ((&T) == (&All))
    return edgeMapDenseFunction(graph, U, h, f, m, c, b);
  using vertex_t = typename fragment_t::vertex_t;
  using vid_t = typename fragment_t::vid_t;
  bool flag = ((&U) == (&All));
  if (!flag)
    U.ToDense();

  U.fw->ForEach(T.s.begin(), T.s.end(),
                [&flag, &graph, &U, &h, &f, &m, &c, &b](int tid, vid_t vid) {
                  vertex_t u;
                  u.SetValue(U.fw->Key2Lid(vid));
                  value_t v = *(U.fw->Get(vid));
                  bool is_update = false;
                  if (!c(vid, v))
                    return;
                  if (h == EU || h == ED) {
                    auto es = graph.GetIncomingAdjList(u);
                    for (auto& e : es) {
                      vid_t nb_gid = graph.Vertex2Gid(e.get_neighbor());
                      vid_t nb_id = U.fw->Gid2Key(nb_gid);
                      if (flag || U.IsIn(nb_id)) {
                        value_t nb = *(U.fw->Get(nb_id));
                        if (f(nb_id, vid, nb, v)) {
                          m(nb_id, vid, nb, v);
                          is_update = true;
                          if (!c(vid, v))
                            break;
                        }
                      }
                    }
                  }
                  if (h == EU || h == ER) {
                    auto es = graph.GetOutgoingAdjList(u);
                    for (auto& e : es) {
                      vid_t nb_gid = graph.Vertex2Gid(e.get_neighbor());
                      vid_t nb_id = U.fw->Gid2Key(nb_gid);
                      if (flag || U.IsIn(nb_id)) {
                        value_t nb = *(U.fw->Get(nb_id));
                        if (f(nb_id, vid, nb, v)) {
                          m(nb_id, vid, nb, v);
                          is_update = true;
                          if (!c(vid, v))
                            break;
                        }
                      }
                    }
                  }
                  if (is_update)
                    U.fw->PutNextPull(vid, v, b, tid);
                });

  VSet res;
  U.fw->Barrier();
  res.is_dense = true;
  U.fw->GetActiveVerticesAndSetStates(res.s, res.d);
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

  U.fw->ForEach(graph.InnerVertices(),
                [&flag, &graph, &U, &h, &f, &m, &c, &b](int tid, vertex_t u) {
                  vid_t vid = U.fw->Lid2Key(u.GetValue());
                  value_t v = *(U.fw->Get(vid));
                  bool is_update = false;
                  if (!c(vid, v))
                    return;
                  if (h == EU || h == ED) {
                    auto es = graph.GetIncomingAdjList(u);
                    for (auto& e : es) {
                      vid_t nb_gid = graph.Vertex2Gid(e.get_neighbor());
                      vid_t nb_id = U.fw->Gid2Key(nb_gid);
                      if (flag || U.IsIn(nb_id)) {
                        value_t nb = *(U.fw->Get(nb_id));
                        if (f(nb_id, vid, nb, v)) {
                          m(nb_id, vid, nb, v);
                          is_update = true;
                          if (!c(vid, v))
                            break;
                        }
                      }
                    }
                  }
                  if (h == EU || h == ER) {
                    auto es = graph.GetOutgoingAdjList(u);
                    for (auto& e : es) {
                      vid_t nb_gid = graph.Vertex2Gid(e.get_neighbor());
                      vid_t nb_id = U.fw->Gid2Key(nb_gid);
                      if (flag || U.IsIn(nb_id)) {
                        value_t nb = *(U.fw->Get(nb_id));
                        if (f(nb_id, vid, nb, v)) {
                          m(nb_id, vid, nb, v);
                          is_update = true;
                          if (!c(vid, v))
                            break;
                        }
                      }
                    }
                  }
                  if (is_update)
                    U.fw->PutNextPull(vid, v, b, tid);
                });

  VSet res;
  U.fw->Barrier();
  res.is_dense = true;
  U.fw->GetActiveVerticesAndSetStates(res.s, res.d);
  return res;
}

template <typename fragment_t, typename value_t, class F, class M, class C, class H>
VSet edgeMapDenseFunction(const fragment_t& graph, VSet& U, H& h, VSet& T,
                          F& f, M& m, C& c, bool b = true) {
  using vid_t = typename fragment_t::vid_t;
  bool flag = ((&U) == (&All));
  if (!flag)
    U.ToDense();

  U.fw->ForEach(T.s.begin(), T.s.end(),
                [&flag, &U, &h, &f, &m, &c, &b](int tid, vid_t vid) {
                  value_t v = *(U.fw->Get(vid));
                  bool is_update = false;
                  if (!c(vid, v))
                    return;
                  auto es = use_edge(h);
                  for (auto& nb_id : es) {
                    if (flag || U.IsIn(nb_id)) {
                      value_t nb = *(U.fw->Get(nb_id));
                      if (f(nb_id, vid, nb, v)) {
                        m(nb_id, vid, nb, v);
                        is_update = true;
                        if (!c(vid, v))
                        break;
                      }
                    }
                  }
                  if (is_update)
                    U.fw->PutNextPull(vid, v, b, tid);
                });

  VSet res;
  U.fw->Barrier();
  res.is_dense = true;
  U.fw->GetActiveVerticesAndSetStates(res.s, res.d);
  return res;
}

template <typename fragment_t, typename value_t, class F, class M, class C, class H>
VSet edgeMapDenseFunction(const fragment_t& graph, VSet& U, H& h, F& f, M& m,
                          C& c, bool b = true) {
  return edgeMapDenseFunction(graph, U, h, All, f, m, c, b);
}

template <typename fragment_t, typename value_t, class F, class M, class C>
VSet doEdgeMapSparse(const fragment_t& graph, VSet& U, int h, F& f, M& m,
                     C& c) {
  using vertex_t = typename fragment_t::vertex_t;
  using vid_t = typename fragment_t::vid_t;

  for (auto& vid : U.s) {
    vertex_t u;
    u.SetValue(U.fw->Key2Lid(vid));
    value_t v = *(U.fw->Get(vid));
    if (h == EU || h == ED) {
      auto es = graph.GetOutgoingAdjList(u);
      for (auto& e : es) {
        vid_t nb_gid = graph.Vertex2Gid(e.get_neighbor());
        vid_t nb_id = U.fw->Gid2Key(nb_gid);
        value_t nb = *(U.fw->Get(nb_id));
        if (c(nb_id, nb) && f(vid, nb_id, v, nb)) {
          m(vid, nb_id, v, nb);
          U.fw->PutNext(nb_id, nb);
        }
      }
    }
    if (h == EU || h == ER) {
      auto es = graph.GetIncomingAdjList(u);
      for (auto& e : es) {
        vid_t nb_gid = graph.Vertex2Gid(e.get_neighbor());
        vid_t nb_id = U.fw->Gid2Key(nb_gid);
        value_t nb = *(U.fw->Get(nb_id));
        if (c(nb_id, nb) && f(vid, nb_id, v, nb)) {
          m(vid, nb_id, v, nb);
          U.fw->PutNext(nb_id, nb);
        }
      }
    }
  }

  /*U.fw->ForEach(U.s.begin(), U.s.end(), [&graph, &U, &h, &f, &m, &c](int tid, vid_t vid) { 
    vertex_t u;
    u.SetValue(U.fw->Key2Lid(vid));
    value_t v = *(U.fw->Get(vid)); 
    if (h == EU || h == ED) {
      auto es = graph.GetOutgoingAdjList(u); 
      for (auto &e: es) { 
        vid_t nb_gid = graph.Vertex2Gid(e.get_neighbor());
        vid_t nb_id = U.fw->Gid2Key(nb_gid);
        value_t nb = *(U.fw->Get(nb_id)); 
        if (c(nb_id, nb) && f(vid, nb_id, v, nb)) {
           m(vid, nb_id, v, nb); 
           U.fw->PutNext(nb_id, nb);
        }
      }
    }
    if (h == EU || h == ER) {
      auto es = graph.GetIncomingAdjList(u);
      for (auto &e: es) {
        vid_t nb_gid = graph.Vertex2Gid(e.get_neighbor());
        vid_t nb_id = U.fw->Gid2Key(nb_gid);
        value_t nb = *(U.fw->Get(nb_id));
        if (c(nb_id, nb) && f(vid, nb_id, v, nb)) {
          m(vid, nb_id, v, nb);
          U.fw->PutNext(nb_id, nb);
        }
      }
    }
  });*/

  VSet res;
  U.fw->Barrier(true);
  U.fw->GetActiveVerticesAndSetStates(res.s);
  return res;
}

template <typename fragment_t, typename value_t, class F, class M, class C,
          class H>
VSet doEdgeMapSparse(const fragment_t& graph, VSet& U, H& h, F& f, M& m,
                     C& c) {
  for (auto& vid : U.s) {
    value_t v = *(U.fw->Get(vid));
    auto es = use_edge(h);
    for (auto& nb_id : es) {
      value_t nb = *(U.fw->Get(nb_id));
      if (c(nb_id, nb) && f(vid, nb_id, v, nb)) {
        m(vid, nb_id, v, nb);
        U.fw->PutNext(nb_id, nb);
      }
    }
  }

  VSet res;
  U.fw->Barrier(true);
  U.fw->GetActiveVerticesAndSetStates(res.s);
  return res;
}

template <typename fragment_t, typename value_t, class F, class M, class C,
          class H>
inline VSet edgeMapSparseFunction(const fragment_t& graph, VSet& U, H h, F& f,
                                  M& m, C& c) {
  U.fw->ResetAggFunc();
  return doEdgeMapSparse(graph, U, h, f, m, c);
}

template <typename fragment_t, typename value_t, class F, class M, class C,
          class R, class H>
inline VSet edgeMapSparseFunction(const fragment_t& graph, VSet& U, H h, F& f,
                                  M& m, C& c, const R& r) {
  U.fw->SetAggFunc(r);
  return doEdgeMapSparse(graph, U, h, f, m, c);
  U.fw->ResetAggFunc();
}

template <typename fragment_t, typename value_t, class F, class M, class C, class H>
inline VSet edgeMapFunction(const fragment_t& graph, VSet& U, H h, F& f, M& m,
                            C& c) {
  int len = VSize(U);
  if (len > THRESHOLD)
    return edgeMapDenseFunction(graph, U, h, f, m, c);
  else
    return edgeMapSparseFunction(graph, U, h, f, m, c);
}

template <typename fragment_t, typename value_t, class F, class M, class C,
          class R, class H>
inline VSet edgeMapFunction(const fragment_t& graph, VSet& U, H h, F& f, M& m,
                            C& c, const R& r) {
  int len = VSize(U);
  if (len > THRESHOLD)
    return edgeMapDenseFunction(graph, U, h, f, m, c);
  else
    return edgeMapSparseFunction(graph, U, h, f, m, c, r);
}

template <typename fragment_t, typename value_t, class F, class M, class C,
          class H>
inline VSet edgeMapFunction(const fragment_t& graph, VSet& U, H h, VSet& T,
                            F& f, M& m, C& c, bool b = true) {
  return edgeMapDenseFunction(graph, U, h, T, f, m, c, b);
}

}  // namespace flash
}  // namespace grape

#endif  // EXAMPLES_ANALYTICAL_APPS_FLASH_APP_API_H_
