from typing import Dict, List, Any, Set
import collections

class CircularDependencyError(Exception):
    pass

def get_insertion_order(schema: Dict[str, Any]) -> List[str]:
    """
    Returns a list of table names in the order they should be populated.
    Determined by topological sort of the dependency graph.
    """
    tables = set(schema.keys())
    adj_list = collections.defaultdict(set)  # Parent -> Children
    in_degree = {t: 0 for t in tables}
    
    # Build graph: Edge A -> B means A must be populated before B
    # A is the referenced table (parent), B is the referencing table (child)
    for child, details in schema.items():
        # dependencies are tables this child refers to
        dependencies = set()
        for fk_info in details.get('foreign_keys', {}).values():
            parent = fk_info['references_table']
            # Ignore self-references for topological sort purposes
            # (Requires special handling during insertion, usually inserting with NULL then updating,
            # or relying on deferrable constraints. For this MVP, we assume simplest case)
            if parent != child and parent in tables:
                dependencies.add(parent)
        
        for parent in dependencies:
            adj_list[parent].add(child)
            in_degree[child] += 1
            
    # Kahn's Algorithm
    queue = collections.deque([t for t in tables if in_degree[t] == 0])
    ordered = []
    
    while queue:
        node = queue.popleft()
        ordered.append(node)
        
        for child in adj_list[node]:
            in_degree[child] -= 1
            if in_degree[child] == 0:
                queue.append(child)
                
    if len(ordered) != len(tables):
        remaining = tables - set(ordered)
        raise CircularDependencyError(
            f"Circular dependency or unresolved references detected involving: {remaining}. "
            "This simple analyzer cannot handle cyclic dependencies yet."
        )
        
    return ordered
