from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Dict
from collections import defaultdict, deque

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get('/')
def read_root():
    return {'Ping': 'Pong'}

def is_dag(nodes: List[str], edges: List[dict]) -> bool:
    # Create a graph and count in-degrees
    in_degree = {node: 0 for node in nodes}
    graph = defaultdict(list)

    # Build graph and calculate in-degrees
    for edge in edges:
        src = edge['source']
        dest = edge['target']
        graph[src].append(dest)
        in_degree[dest] += 1

    # Queue for processing nodes
    queue = deque([node for node in nodes if in_degree[node] == 0])

    visited_count = 0

    while queue:
        current = queue.popleft()
        visited_count += 1

        # Decrease in-degree for all neighbors
        for neighbor in graph[current]:
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                queue.append(neighbor)

    # If we visited all nodes, the graph is a DAG
    return visited_count == len(nodes)

@app.post('/pipelines/parse')
def parse_pipeline(pipeline: dict):
    num_nodes = len(pipeline["nodes"])
    num_edges = len(pipeline["edges"])
    
    node_ids = [node["id"] for node in pipeline["nodes"]]
    is_dag_result = is_dag(node_ids, pipeline["edges"])

    return {'num_nodes': num_nodes, 'num_edges': num_edges, 'is_dag': is_dag_result}
