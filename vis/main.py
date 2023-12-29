import networkx as nx
import matplotlib.pyplot as plt
import math
import matplotlib.patches as patches

G = nx.DiGraph()

colors = ['yellow', 'cyan', 'lightgreen']
# Example: Add 10 nodes with level and color properties
node_count = 20
edge_count = node_count * 2
for i in range(1, 11):
    G.add_node(i, level=int(i / 4) + 1, color=colors[i % 3])
# Add 20 edges (example: random connections)
G.add_edges_from([(1, 2), (2, 3), (4, 5), (1,4), (5,7), (3,6), (6,8), (7, 9), (8, 10), (5, 10), (3, 9)])  # Add your edges here

max_level = max(nx.get_node_attributes(G, 'level').values())  # Get the highest level
radius_step = 1 / max_level  # Radius increment for each level

def initialize_positions(G):    
    positions = {}
    level_node_count = {level: 0 for level in range(1, max_level + 1)}

    for node in G.nodes(data=True):
        level = node[1]['level']
        radius = level * radius_step
        level_node_count[level] += 1
        angle = 2 * math.pi * level_node_count[level] / len([n for n in G.nodes() if G.nodes[n]['level'] == level])
        x = radius * math.cos(angle)
        y = radius * math.sin(angle)
        positions[node[0]] = (x, y)

    return positions

# Initialize Positions
layout = initialize_positions(G)

# Subplot for 4 stages
fig, axes = plt.subplots(2, 2, figsize=(10, 10))

def iteration(times):
    for _ in range(times):
        for node in G.nodes():
            x, y = layout[node]
            level = G.nodes[node]['level']
            radius = level * radius_step
            dx, dy = 0, 0
            for neighbor in G.neighbors(node):
                level_diff = abs(G.nodes[node]['level'] - G.nodes[neighbor]['level'])
                desired_distance = level_diff * 1

                actual_distance = math.sqrt((x - layout[neighbor][0]) ** 2 + 
                                            (y - layout[neighbor][1]) ** 2)
                distance_diff = actual_distance - desired_distance

                if actual_distance > 0:
                    dx += (x - layout[neighbor][0]) / actual_distance * distance_diff
                    dy += (y - layout[neighbor][1]) / actual_distance * distance_diff

            learning_rate = 0.01
            # Calculate new position
            new_x = x - learning_rate * dx
            new_y = y - learning_rate * dy

            # Adjust new position to be on the circle
            angle = math.atan2(new_y, new_x)
            layout[node] = (radius * math.cos(angle), radius * math.sin(angle))


# Optimization and Drawing for 5, 10, 15, 20 iterations
interval = 30
iteration_steps = [0, interval, interval*2, interval*3]
for i in range(len(iteration_steps)):
    if i > 0:
        iteration(iteration_steps[i] - iteration_steps[i-1])

    ax = axes[int(i/2)][i%2]
    for level in range(1, max_level + 1):
        radius = level * radius_step
        circle = patches.Circle((0, 0), radius, fill=False, color='gray', ls='dashed')
        ax.add_patch(circle)
    ax.set_aspect('equal', 'box')
    nx.draw(G, layout, with_labels=True, node_color=[G.nodes[n]['color'] for n in G.nodes()], ax=ax, arrows=True)
    ax.set_title(f'Iteration {iteration_steps[i]}')

plt.savefig('graph.png')
