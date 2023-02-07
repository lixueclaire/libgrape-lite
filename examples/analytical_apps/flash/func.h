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

#ifndef EXAMPLES_ANALYTICAL_APPS_FLASH_FUNC_H_
#define EXAMPLES_ANALYTICAL_APPS_FLASH_FUNC_H_

namespace grape {
namespace flash {

#define EPS 1e-10
#define EU -1
#define ED -2
#define ER -3
#define THRESHOLD (VSize(All) / 50)

#define VSet VertexSubset<fragment_t, value_t>
#define All VertexSubset<fragment_t, value_t>::all
#define EmptySet VertexSubset<fragment_t, value_t>::empty

#define GetV(id) All.fw->Get(id)
#define OutDeg(id) getOutDegree<fragment_t, vid_t>(graph, All.fw->Key2Lid(id))
#define InDeg(id) getInDegree<fragment_t, vid_t>(graph, All.fw->Key2Lid(id))
#define Deg(id) (OutDeg(id) + InDeg(id))
#define Print(...)  if (All.fw->GetPid() == 0) printf(__VA_ARGS__)

#define VSize(U) U.size()
#define VertexMap(U, f, ...) vertexMapFunction(graph, U, f, ##__VA_ARGS__)
#define EdgeMap(U, H, F, M, C, ...) \
  edgeMapFunction(graph, U, H, F, M, C, ##__VA_ARGS__)
#define EdgeMapDense(U, H, F, M, C, ...) \
  edgeMapDenseFunction(graph, U, H, F, M, C, ##__VA_ARGS__)
#define EdgeMapSparse(U, H, F, M, C, ...) \
  edgeMapSparseFunction(graph, U, H, F, M, C, ##__VA_ARGS__)

#define DefineFV(F) auto F = [&](const vid_t id, const value_t& v) -> bool
#define DefineMapV(F) auto F = [&](const vid_t id, value_t& v)
#define DefineFE(F)                                                \
  auto F = [&](const vid_t sid, const vid_t did, const value_t& s, \
               const value_t& d) -> bool
#define DefineMapE(F) \
  auto F = [&](const vid_t sid, const vid_t did, const value_t& s, value_t& d)
#define CTrueV cTrueV<vid_t, value_t>
#define CTrueE cTrueE<vid_t, value_t>

#define EjoinV(E, V) E, V
#define VjoinP(property) std::vector<vid_t> res; res.push_back(v.property); return res;
#define DefineOutEdges(F) auto F=[&](value_t& v) -> std::vector<vid_t>
#define DefineInEdges(F) auto F=[&](value_t& v) -> std::vector<vid_t>
#define use_edge(F) F(v)

template <typename vid_t, typename value_t>
inline bool cTrueV(const vid_t id, const value_t& v) {
  return true;
}
template <typename vid_t, typename value_t>
inline bool cTrueE(const vid_t sid, const vid_t did, const value_t& s,
                   const value_t& d) {
  return true;
}

template <typename fragment_t, typename vid_t>
inline int getOutDegree(const fragment_t& graph, vid_t lid) {
  Vertex<vid_t> v;
  v.SetValue(lid);
  return graph.GetLocalOutDegree(v);
}
template <typename fragment_t, typename vid_t>
inline int getInDegree(const fragment_t& graph, vid_t lid) {
  Vertex<vid_t> v;
  v.SetValue(lid);
  return graph.GetLocalInDegree(v);
}

}  // namespace flash
}  // namespace grape

#endif  // EXAMPLES_ANALYTICAL_APPS_FLASH_FUNC_H_
