import pyspark
import itertools
import copy
import sys
import time
st = time.time()
threshold=int(sys.argv[1])
input_path=str(sys.argv[2])
output_path1=str(sys.argv[3])
output_path2=str(sys.argv[4])

sc = pyspark.SparkContext()
def iterate_vertexes(root, matrix):
    visited = []
    dict_visited = dict()
    future_visit = [root]
    current_node = {}
    current_edge = {}

    while (future_visit):
        vertex = future_visit.pop(0)
        # find bfs
        if vertex == root:
            dict_visited[root] = [[], 0]
            visited.append(root)
        else:
            visited.append(vertex)
        # taking only nodes that are not yet visited
        to_update = matrix[vertex] - set(visited)
        # taking only unique nodes
        for nodes in to_update - set(future_visit):
            future_visit.append(nodes)

        # find the shortest path from root to each node
        for x in to_update:
            if dict_visited.get(x):
                if dict_visited[x][1] == dict_visited[vertex][1] + 1:
                    dict_visited[x][0].append(vertex)
            else:
                dict_visited[x] = [[vertex], dict_visited[vertex][1] + 1]

    # start reading the nodes from leaf and calculate the edge and node betweenness
    for node in visited[::-1]:
        current_node[node] = 1 if not current_node.get(node) else current_node.get(node) + 1
        parent_node = dict_visited[node][0]
        # divide the edge credit
        if len(parent_node):
            edge_credit = float(current_node[node]) / len(parent_node)

        for p in parent_node:
            current_edge[(node, p)] = edge_credit
            current_node[p] = current_node.get(p) + edge_credit if current_node.get(p) else edge_credit
    result = []
    for k, v in current_edge.items():
        result.append((k, v))
    return result


def generate_community(communities, edge):

    # deleting the edge from the adjacency matrix
    updated_matrix[edge[0]] = updated_matrix[edge[0]] - set([edge[1]])
    updated_matrix[edge[1]] = updated_matrix[edge[1]] - set([edge[0]])

    big_community, check_node = (updated_matrix[edge[0]], edge[1])

    # checking if there are distinct communities
    for n in big_community:
        if check_node in updated_matrix[n]:
            return len(communities), None, communities

    # checking if there is a connection through bfs of  node 1
    visited = []
    future_visit = [edge[1]]
    while (future_visit):
        vertex = future_visit.pop(0)
        visited.append(vertex)
        # taking only nodes that are not yet visited
        to_update = updated_matrix[vertex] - set(visited)

        for nodes in to_update - set(future_visit):
            future_visit.append(nodes)
    # print("visited",visited)
    if edge[0] in visited:
        return len(communities), None, communities
    else:
        # if more then one community exists
        communities_new = []
        for i, each_com in enumerate(communities):
            if set(edge).issubset(each_com):

                split = [each_com - set(visited), set(visited)]
                communities_new += split
            else:
                communities_new.append(each_com)
        sing_mod=[]
        for ch in communities_new:
            local_mod = 0
            for pair in itertools.combinations(ch, 2):
                aij = 0 if pair[0] not in matrix[pair[1]] else 1
                k_i = len(matrix[pair[0]])
                k_j = len(matrix[pair[1]])
                local_mod += (aij - (float(k_i * k_j) / (2 * m)))
            sing_mod.append(local_mod)

        final_mod = float(sum(sing_mod)) / (2 * m)

        return len(communities_new), final_mod, communities_new


rdd = sc.textFile(input_path)
header = rdd.first()
df = rdd.filter(lambda x: x != header).map(lambda x: x.split(',')).groupByKey().filter(lambda x: len(x[1]) >= threshold).cache()
pairs = []
df2 = df.collectAsMap()
# generating all the pairs which have rated same businesses >=7
for i in itertools.combinations(df.map(lambda x: x[0]).collect(), 2):
    intersection_pairs = set(df2[i[0]]).intersection(set(df2[i[1]]))
    if len(intersection_pairs) >= threshold:
        pairs.append(i)

matrix = sc.parallelize(pairs).flatMap(lambda x: [(x[0], x[1]), (x[1], x[0])]).groupByKey().mapValues(
    set).collectAsMap()
res = sc.parallelize(matrix.keys()).flatMap(lambda x: iterate_vertexes(x, matrix)).map(
    lambda x: (tuple(sorted(x[0])), x[1])).reduceByKey(lambda x, y: x + y).map(lambda x: (x[0], x[1] / 2)).cache()


with open (output_path1,'w') as f:
    for i in res.map(lambda x : (tuple(sorted(x[0])),x[1])).sortBy(lambda x:x[0]).sortBy(lambda x:x[1],ascending=False). collect():
        f.write(str(i[0])+","+str(i[1])+"\n")




sorted_edges = res.sortBy(lambda x: x[1], ascending=False).collect()
mod_dict = {}
updated_matrix = copy.deepcopy(matrix)
communities = [set(matrix.keys())]
max_ = -0.000001
final_comm = []
m = len(sorted_edges)
bfs_community = []

while communities[0]:
    visited = []
    bfs_node=list(communities[0])[0]
    future_visit = [bfs_node]
    while (future_visit):
        vertex = future_visit.pop(0)
        visited.append(vertex)
        # taking only nodes that are not yet visited
        to_update = matrix[vertex] - set(visited)
        for nodes in to_update - set(future_visit):
            future_visit.append(nodes)
    communities=[communities[0]-set(visited)]
    bfs_community.append(set(visited))
communities=bfs_community

while sorted_edges:
    no_com, mod_value, communities = generate_community(communities, sorted_edges[0][0])
    # print("nnnn",communities)
    if mod_value:
        if max_ < mod_value:
            max_ = mod_value
            final_comm = communities
    sorted_edges = sc.parallelize(updated_matrix.keys()).flatMap(lambda x: iterate_vertexes(x, updated_matrix)).map(
        lambda x: (tuple(sorted(x[0])), x[1])).reduceByKey(lambda x, y: x + y).map(lambda x: (x[0], x[1] / 2)).sortBy(
        lambda x: x[1], ascending=False).collect()

ans=sc.parallelize(final_comm).map(lambda x:sorted(x)).sortBy(lambda x:(len(x),list(x[0]))).collect()
with open (output_path2,'w') as f:
    for i in ans:
        for j in i:
            f.write("'"+j+"',")
        f.seek(f.tell()-1)
        f.write("\n")
et = time.time()
print(et - st)
