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
#define GetMax(A, B) All.fw->Max(A, B)
#define GetMin(A, B) All.fw->Min(A, B)
#define GetSum(A, B) All.fw->Sum(A, B)

#define GetV(id) All.fw->Get(id)
#define OutDeg(id) getOutDegree<fragment_t, vid_t>(graph, All.fw->Key2Lid(id))
#define InDeg(id) getInDegree<fragment_t, vid_t>(graph, All.fw->Key2Lid(id))
#define Deg(id) (OutDeg(id) + InDeg(id))
#define VDATA(id) getVData<fragment_t, vid_t>(graph, All.fw->Key2Lid(id))
#define Print(...)  if (All.fw->GetPid() == 0) printf(__VA_ARGS__)

#define for_in(...) { vertex_t u; \
                      u.SetValue(All.fw->Key2Lid(id)); \
                      auto es = graph.GetIncomingAdjList(u); \
                      for (auto& e : es) { \
                        vid_t nb_id = graph.Vertex2Gid(e.get_neighbor()); \
                        edata_t weight = e.get_data(); \
                        nb_id = All.fw->Gid2Key(nb_id); \
                        value_t nb = *(All.fw->Get(nb_id)); \
                        __VA_ARGS__ \
                      } }
#define for_out(...) {vertex_t u; \
                      u.SetValue(All.fw->Key2Lid(id)); \
                      auto es = graph.GetOutgoingAdjList(u); \
                      for (auto& e : es) { \
                        vid_t nb_id = graph.Vertex2Gid(e.get_neighbor()); \
                        edata_t weight = e.get_data(); \
                        nb_id = All.fw->Gid2Key(nb_id); \
                        value_t nb = *(All.fw->Get(nb_id)); \
                        __VA_ARGS__ \
                      } }
#define for_nb(...) {for_in(__VA_ARGS__) for_out(__VA_ARGS__)}

#define VSize(U) U.size()
#define VertexMap(U, f, ...) vertexMapFunction(graph, U, f, ##__VA_ARGS__)
#define VertexMapSeq(U, f, ...) vertexMapSeqFunction(graph, U, f, ##__VA_ARGS__)
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
               const value_t& d, const edata_t& weight) -> bool
#define DefineMapE(F) \
  auto F = [&](const vid_t sid, const vid_t did, const value_t& s, \
               value_t& d, const edata_t& weight)
#define CTrueV cTrueV<vid_t, value_t>
#define CTrueE cTrueE<vid_t, value_t, edata_t>

#define EjoinV(E, V) E, V
#define VjoinP(property) std::vector<vid_t> res; res.push_back(v.property); return res;
#define DefineOutEdges(F) auto F=[&](const vid_t vid, const value_t& v) -> std::vector<vid_t>
#define DefineInEdges(F) auto F=[&](const vid_t vid, const value_t& v) -> std::vector<vid_t>
#define use_edge(F) F(vid, v)

template <class T> int set_intersect(const std::vector<T> &x,
                                     const std::vector<T> &y,
                                     std::vector<T> &v) {
  auto it = set_intersection(x.begin(), x.end(), y.begin(), y.end(), v.begin());
  return it - v.begin();
}

template <class T1, class T2> void add(std::vector<T1> &x, std::vector<T1> &y, T2 c) {
  for (size_t i = 0; i < x.size(); ++i) 
    x[i] += y[i] * c;
}
template <class T> void add(std::vector<T> &x, std::vector<T> &y) {
  for (size_t i = 0; i < x.size(); ++i) 
    x[i] += y[i];
}
template <class T> T prod(std::vector<T> &x, std::vector<T> &y) {
  T s = 0; 
  for (size_t i = 0; i < x.size(); ++i)
    s += x[i] * y[i];
    return s;
}
template <class T1, class T2> void mult(std::vector<T1> &v, T2 c) {
  for(size_t i = 0; i < v.size(); ++i) 
    v[i] *= c;
}

template <typename vid_t, typename value_t>
inline bool cTrueV(const vid_t id, const value_t& v) {
  return true;
}
template <typename vid_t, typename value_t, typename edata_t>
inline bool cTrueE(const vid_t sid, const vid_t did, const value_t& s,
                   const value_t& d, const edata_t& weight) {
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

template <typename fragment_t, typename vid_t>
inline const typename fragment_t::vdata_t& getVData(const fragment_t& graph, vid_t lid) {
  Vertex<vid_t> v;
  v.SetValue(lid);
  return graph.GetData(v);
}

}  // namespace flash
}  // namespace grape

#endif  // EXAMPLES_ANALYTICAL_APPS_FLASH_FUNC_H_
