from collections import defaultdict
import pagerank
import sqlite3
import sys

USAGE = 'Usage: python3 influence.py [--db <db_name>]'

def main():
    db = 'sc_graph.sqlite3'
    if len(sys.argv) == 3:
        db = sys.argv[2]
    elif len(sys.argv) > 3 or len(sys.argv) == 2:
        print(USAGE)

    G = build_graph_from_db(db)
    print('Finished building graph. Ranking...')
    ranking = pagerank.rank(G)

    for user, rank in ranking.iteritems():
        print("{}: {}".format(user, rank))

def build_graph_from_db(db):
    # TODO: Build a nested dictionary where each key (user) maps to a dict of where 
    #   keys are target users and values are weights
    conn = sqlite3.connect(db)

    graph = defaultdict(lambda : defaultdict(int))

    user_ids = [str(user_id[0]) for user_id in conn.execute('SELECT id FROM users')]

    for user_id in user_ids:
        following = [following_id[0] for following_id in conn.execute('SELECT following_id FROM following WHERE id = ?', (user_id,))]
        commented_on = [target_id[0] for target_id in conn.execute('SELECT target_id FROM comments WHERE id = ?', (user_id,))]
        likes = [target_id[0] for target_id in conn.execute('SELECT target_id FROM likes WHERE id = ?', (user_id,))]
        reposted = [target_id[0] for target_id in conn.execute('SELECT target_id FROM reposts WHERE id = ?', (user_id,))]

        combined = following + commented_on + likes + reposted
        for target in combined:
            graph[user_id][target] += 1

        print('{} has {} outgoing edges'.format(user_id, len(graph[user_id])))

    # Example graph: {'diplo': {'lil uzi vert': 2, 'major lazer': 16}, 'lil uzi vert': {'diplo': 5}}
    return graph

if __name__ == '__main__':
    main()
    G = build_graph_from_db()