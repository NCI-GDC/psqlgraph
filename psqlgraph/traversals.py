from collections import deque, namedtuple


def traverse(root, mode="bfs", max_depth=None, edge_pointer="in", edge_predicate=None):
    """
    Performs a traversal starting at the current node
    Args:
        root (Node): root node to start traverse
        mode (str): type of traversal, defaults to breadth first search
        max_depth (int): maximum distance to traverse
        edge_pointer (str): Determines what edge direction to use, possible values are `in`, `out`
                        `in`: use node.edges_in, default behavior
        edge_predicate (func): a predicate performed on an `edge` object in
        order to decided whether to walk that edge or not

    Returns:
        generator: nodes found in the sub tree
    """
    if mode == "bfs":
        return _bfs(
            root=root,
            edge_predicate=edge_predicate,
            edge_pointer=edge_pointer,
            max_depth=max_depth
        )

    if mode == "dfs":
        return _dfs(
            root=root,
            edge_predicate=edge_predicate,
            edge_pointer=edge_pointer,
            max_depth=max_depth
        )

    raise NotImplementedError("Traversal mode {} is not implemented".format(mode))


def _bfs(root, edge_predicate=None, max_depth=None, edge_pointer="in"):
    """
    Perform a BFS, with `self` being the root node

    root (Node): root node to start traverse
    :param edge_predicate: a predicate performed on an `edge` object in
        order to decided whether to walk that edge or not
    :type edge_predicate: func
    :param max_depth: maximum distance to traverse
    :type max_depth: int
    :param edge_pointer: possible values `in`, `out`
                        `in`: use node.edges_in, default behavior
                        `out`: use edges_out
    :type edge_pointer: str

    :return: generator
    """

    if not callable(edge_predicate):
        def edge_predicate(e):
            return True

    if max_depth is None:
        max_depth = float('inf')

    marked = set()
    queue = deque([(root, 0)])

    marked.add(root.node_id)

    while queue:
        current, depth = queue.popleft()

        yield current

        if depth + 1 > max_depth:
            continue

        edges = current.edges_out if edge_pointer == "out" else current.edges_in
        for edge in edges:
            if not edge_predicate(edge):
                continue

            n = edge.dst if edge_pointer == "out" else edge.src

            if n.node_id not in marked:
                queue.append((n, depth + 1))
                marked.add(n.node_id)


def _dfs(root, edge_predicate=None, max_depth=None, edge_pointer="in"):
    """
    Perform a DFS, with `self` being the root node

    To implement max depth, some node are visited more than once to update shortest
    path. But those node should only be yield once.

    root (Node): root node to start traverse
    :param edge_predicate: a predicate performed on an `edge` object in
        order to decided whether to walk that edge or not
    :type edge_predicate: func
    :param max_depth: maximum distance to traverse
    :type max_depth: int
    :param edge_pointer: possible values `in`, `out`
                        `in`: use node.edges_in, default behavior
                        `out`: use edges_out
    :type edge_pointer: str

    :return: generator
    """
    StackItem = namedtuple('StackItem', ['node', 'next_child', 'level'])
    edge_predicate = edge_predicate if callable(edge_predicate) else lambda _: True
    max_depth = float('inf') if max_depth is None else max_depth

    visited = {root.node_id: 0}
    yield root
    stack = [StackItem(root, 0, 0)]

    while stack:
        node, next_child, level = stack.pop()
        if level >= max_depth:
            continue
        edges = node.edges_out if edge_pointer == "out" else node.edges_in

        edges_with_index = (
            (index, edges[index])
            for index in range(next_child, len(edges))
            if edge_predicate(edges[index])
        )

        for index, edge in edges_with_index:
            n = edge.dst if edge_pointer == "out" else edge.src
            if n.node_id not in visited:
                yield n
            elif max_depth == float('inf') or level + 1 >= visited[n.node_id]:
                continue
            # update levels for max_depth if shorter path found
            # but do not yield node again

            stack.append(StackItem(node, index + 1, level))
            visited[n.node_id] = level + 1
            stack.append(StackItem(n, 0, level + 1))
            break
